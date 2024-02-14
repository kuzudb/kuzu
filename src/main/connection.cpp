#include "main/connection.h"

#include <utility>

#include "binder/binder.h"
#include "common/exception/connection.h"
#include "common/random_engine.h"
#include "main/database.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "parser/visitor/statement_read_write_analyzer.h"
#include "planner/operator/logical_plan_util.h"
#include "planner/planner.h"
#include "processor/plan_mapper.h"
#include "processor/processor.h"
#include "transaction/transaction_context.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

Connection::Connection(Database* database) {
    KU_ASSERT(database != nullptr);
    this->database = database;
    clientContext = std::make_unique<ClientContext>(database);
}

Connection::~Connection() {}

void Connection::setMaxNumThreadForExec(uint64_t numThreads) {
    clientContext->numThreadsForExecution = numThreads;
}

uint64_t Connection::getMaxNumThreadForExec() {
    std::unique_lock<std::mutex> lck{mtx};
    return clientContext->numThreadsForExecution;
}

std::unique_ptr<PreparedStatement> Connection::prepare(std::string_view query) {
    std::unique_lock<std::mutex> lck{mtx};
    return prepareNoLock(query);
}

std::unique_ptr<QueryResult> Connection::query(std::string_view query) {
    lock_t lck{mtx};
    auto preparedStatement = prepareNoLock(query);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

std::unique_ptr<QueryResult> Connection::query(
    std::string_view query, std::string_view encodedJoin) {
    lock_t lck{mtx};
    auto preparedStatement = prepareNoLock(query, true /* enumerate all plans */, encodedJoin);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

std::unique_ptr<QueryResult> Connection::queryResultWithError(std::string_view errMsg) {
    auto queryResult = std::make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(
    std::string_view query, bool enumerateAllPlans, std::string_view encodedJoin) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    if (query.empty()) {
        preparedStatement->success = false;
        preparedStatement->errMsg = "Connection Exception: Query is empty.";
        return preparedStatement;
    }
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    std::unique_ptr<Statement> statement;
    try {
        statement = Parser::parseQuery(query);
        preparedStatement->preparedSummary.statementType = statement->getStatementType();
        preparedStatement->readOnly = parser::StatementReadWriteAnalyzer().isReadOnly(*statement);
        if (database->systemConfig.readOnly && !preparedStatement->isReadOnly()) {
            throw ConnectionException("Cannot execute write operations in a read-only database!");
        }
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
        compilingTimer.stop();
        preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
        return preparedStatement;
    }
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<LogicalPlan> logicalPlan;
    try {
        // parsing
        if (statement->getStatementType() != StatementType::TRANSACTION) {
            auto txContext = clientContext->transactionContext.get();
            if (txContext->isAutoTransaction()) {
                txContext->beginAutoTransaction(preparedStatement->readOnly);
            } else {
                txContext->validateManualTransaction(
                    preparedStatement->allowActiveTransaction(), preparedStatement->readOnly);
            }
            if (!clientContext->getTx()->isReadOnly()) {
                database->catalog->initCatalogContentForWriteTrxIfNecessary();
                database->storageManager->initStatistics();
            }
        }
        // binding
        auto binder = Binder(*database->catalog, database->memoryManager.get(),
            database->storageManager.get(), database->vfs.get(), clientContext.get(),
            database->extensionOptions.get());
        auto boundStatement = binder.bind(*statement);
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->statementResult =
            std::make_unique<BoundStatementResult>(boundStatement->getStatementResult()->copy());
        // planning
        auto planner = Planner(database->catalog.get(), database->storageManager.get());
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        if (enumerateAllPlans) {
            plans = planner.getAllPlans(*boundStatement);
        } else {
            plans.push_back(planner.getBestPlan(*boundStatement));
        }
        // optimizing
        for (auto& plan : plans) {
            optimizer::Optimizer::optimize(plan.get(), clientContext.get());
        }
        if (!encodedJoin.empty()) {
            std::unique_ptr<LogicalPlan> match;
            for (auto& plan : plans) {
                if (LogicalPlanUtil::encodeJoin(*plan) == encodedJoin) {
                    match = std::move(plan);
                }
            }
            if (match == nullptr) {
                throw ConnectionException(
                    stringFormat("Cannot find a plan matching {}", encodedJoin));
            }
            preparedStatement->logicalPlans.push_back(std::move(match));
        } else {
            preparedStatement->logicalPlans = std::move(plans);
        }
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
        clientContext->transactionContext->rollback();
    }
    compilingTimer.stop();
    preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
    return preparedStatement;
}

void Connection::interrupt() {
    clientContext->interrupt();
}

void Connection::setQueryTimeOut(uint64_t timeoutInMS) {
    lock_t lck{mtx};
    clientContext->timeoutInMS = timeoutInMS;
}

uint64_t Connection::getQueryTimeOut() {
    lock_t lck{mtx};
    return clientContext->timeoutInMS;
}

std::unique_ptr<QueryResult> Connection::executeWithParams(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>>
        inputParams) { // NOLINT(performance-unnecessary-value-param): It doesn't make sense to pass
                       // the map as a const reference.
    lock_t lck{mtx};
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, inputParams);
    } catch (Exception& exception) {
        std::string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement);
}

void Connection::bindParametersNoLock(PreparedStatement* preparedStatement,
    const std::unordered_map<std::string, std::unique_ptr<Value>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, value] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw Exception("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        if (*expectParam->getDataType() != *value->getDataType()) {
            throw Exception("Parameter " + name + " has data type " +
                            value->getDataType()->toString() + " but expects " +
                            expectParam->getDataType()->toString() + ".");
        }
        // The much more natural `parameterMap.at(name) = std::move(v)` fails.
        // The reason is that other parts of the code rely on the existing Value object to be
        // modified in-place, not replaced in this map.
        *parameterMap.at(name) = std::move(*value);
    }
}

std::unique_ptr<QueryResult> Connection::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx) {
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    if (preparedStatement->preparedSummary.statementType != common::StatementType::TRANSACTION &&
        clientContext->getTx() == nullptr) {
        clientContext->transactionContext->beginAutoTransaction(preparedStatement->isReadOnly());
        if (!preparedStatement->readOnly) {
            database->catalog->initCatalogContentForWriteTrxIfNecessary();
            database->storageManager->initStatistics();
        }
    }
    clientContext->resetActiveQuery();
    clientContext->startTimingIfEnabled();
    auto mapper = PlanMapper(*database->storageManager, database->memoryManager.get(),
        database->catalog.get(), clientContext.get());
    std::unique_ptr<PhysicalPlan> physicalPlan;
    if (preparedStatement->isSuccess()) {
        try {
            physicalPlan =
                mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[planIdx].get(),
                    preparedStatement->statementResult->getColumns());
        } catch (std::exception& exception) {
            clientContext->transactionContext->rollback();
            return queryResultWithError(exception.what());
        }
    }
    auto queryResult = std::make_unique<QueryResult>(preparedStatement->preparedSummary);
    auto profiler = std::make_unique<Profiler>();
    auto executionContext = std::make_unique<ExecutionContext>(profiler.get(), clientContext.get());
    profiler->enabled = preparedStatement->isProfile();
    auto executingTimer = TimeMetric(true /* enable */);
    executingTimer.start();
    std::shared_ptr<FactorizedTable> resultFT;
    try {
        if (preparedStatement->isTransactionStatement()) {
            resultFT =
                database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
        } else {
            if (clientContext->transactionContext->isAutoTransaction()) {
                resultFT =
                    database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
                clientContext->transactionContext->commit();
            } else {
                resultFT =
                    database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
            }
        }
    } catch (Exception& exception) {
        clientContext->transactionContext->rollback();
        return queryResultWithError(std::string(exception.what()));
    }
    executingTimer.stop();
    queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
    queryResult->initResultTableAndIterator(
        std::move(resultFT), preparedStatement->statementResult->getColumns());
    return queryResult;
}

void Connection::addScalarFunction(std::string name, function::function_set definitions) {
    database->catalog->addFunction(std::move(name), std::move(definitions));
}

bool Connection::startUDFAutoTrx(transaction::TransactionContext* trx) {
    if (!trx->hasActiveTransaction()) {
        auto res = query("BEGIN TRANSACTION");
        KU_ASSERT(res->isSuccess());
        return true;
    }
    return false;
}

void Connection::commitUDFTrx(bool isAutoCommitTrx) {
    if (isAutoCommitTrx) {
        auto res = query("COMMIT");
        KU_ASSERT(res->isSuccess());
    }
}

} // namespace main
} // namespace kuzu
