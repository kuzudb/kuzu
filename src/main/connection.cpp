#include "main/connection.h"

#include "binder/binder.h"
#include "common/exception/connection.h"
#include "main/database.h"
#include "optimizer/optimizer.h"
#include "parser//visitor/statement_read_write_analyzer.h"
#include "parser/parser.h"
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

void Connection::beginReadOnlyTransaction() {
    query("BEGIN TRANSACTION READ ONLY");
}

void Connection::beginWriteTransaction() {
    query("BEGIN TRANSACTION");
}

void Connection::commit() {
    query("COMMIT");
}

void Connection::rollback() {
    query("ROLLBACK");
}

void Connection::setMaxNumThreadForExec(uint64_t numThreads) {
    clientContext->numThreadsForExecution = numThreads;
}

uint64_t Connection::getMaxNumThreadForExec() {
    std::unique_lock<std::mutex> lck{mtx};
    return clientContext->numThreadsForExecution;
}

std::unique_ptr<PreparedStatement> Connection::prepare(const std::string& query) {
    std::unique_lock<std::mutex> lck{mtx};
    return prepareNoLock(query);
}

std::unique_ptr<QueryResult> Connection::query(const std::string& query) {
    lock_t lck{mtx};
    auto preparedStatement = prepareNoLock(query);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

std::unique_ptr<QueryResult> Connection::query(
    const std::string& query, const std::string& encodedJoin) {
    lock_t lck{mtx};
    auto preparedStatement = prepareNoLock(query, true /* enumerate all plans */, encodedJoin);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

std::unique_ptr<QueryResult> Connection::queryResultWithError(const std::string& errMsg) {
    auto queryResult = std::make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(
    const std::string& query, bool enumerateAllPlans, std::string encodedJoin) {
    auto preparedStatement = std::make_unique<PreparedStatement>();
    if (query.empty()) {
        preparedStatement->success = false;
        preparedStatement->errMsg = "Connection Exception: Query is empty.";
        return preparedStatement;
    }
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    std::unique_ptr<ExecutionContext> executionContext;
    std::unique_ptr<LogicalPlan> logicalPlan;
    try {
        // parsing
        auto statement = Parser::parseQuery(query);
        preparedStatement->readOnly = parser::StatementReadWriteAnalyzer().isReadOnly(*statement);
        // binding
        auto binder = Binder(*database->catalog, database->memoryManager.get(),
            database->storageManager.get(), clientContext.get());
        auto boundStatement = binder.bind(*statement);
        preparedStatement->preparedSummary.statementType = boundStatement->getStatementType();
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->statementResult = boundStatement->getStatementResult()->copy();
        // planning
        auto nodeStatistics = database->storageManager->getNodesStatisticsAndDeletedIDs();
        auto relStatistics = database->storageManager->getRelsStatistics();
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        if (enumerateAllPlans) {
            plans = Planner::getAllPlans(
                *database->catalog, *nodeStatistics, *relStatistics, *boundStatement);
        } else {
            plans.push_back(Planner::getBestPlan(
                *database->catalog, *nodeStatistics, *relStatistics, *boundStatement));
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
                throw ConnectionException("Cannot find a plan matching " + encodedJoin);
            }
            preparedStatement->logicalPlans.push_back(std::move(match));
        } else {
            preparedStatement->logicalPlans = std::move(plans);
        }
    } catch (std::exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
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
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams) {
    lock_t lck{mtx};
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, std::move(inputParams));
    } catch (Exception& exception) {
        std::string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement);
}

void Connection::bindParametersNoLock(PreparedStatement* preparedStatement,
    std::unordered_map<std::string, std::unique_ptr<Value>> inputParams) {
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
    checkPreparedStatementAccessMode(preparedStatement);
    clientContext->resetActiveQuery();
    clientContext->startTimingIfEnabled();
    auto mapper = PlanMapper(
        *database->storageManager, database->memoryManager.get(), database->catalog.get());
    std::unique_ptr<PhysicalPlan> physicalPlan;
    if (preparedStatement->isSuccess()) {
        try {
            physicalPlan =
                mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[planIdx].get(),
                    preparedStatement->statementResult->getColumns());
        } catch (std::exception& exception) {
            preparedStatement->success = false;
            preparedStatement->errMsg = exception.what();
        }
    }
    if (!preparedStatement->isSuccess()) {
        clientContext->transactionContext->rollback();
        return queryResultWithError(preparedStatement->errMsg);
    }
    auto queryResult = std::make_unique<QueryResult>(preparedStatement->preparedSummary);
    auto profiler = std::make_unique<Profiler>();
    auto executionContext =
        std::make_unique<ExecutionContext>(clientContext->numThreadsForExecution, profiler.get(),
            database->memoryManager.get(), database->bufferManager.get(), clientContext.get());
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
                clientContext->transactionContext->beginAutoTransaction(
                    preparedStatement->isReadOnly());
                resultFT =
                    database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
                clientContext->transactionContext->commit();
            } else {
                clientContext->transactionContext->validateManualTransaction(
                    preparedStatement->allowActiveTransaction(), preparedStatement->isReadOnly());
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
    database->catalog->addFunction(name, std::move(definitions));
}

void Connection::checkPreparedStatementAccessMode(PreparedStatement* preparedStatement) {
    if (database->systemConfig.readOnly && !preparedStatement->isReadOnly()) {
        throw ConnectionException("Cannot execute write operations in a read-only database!");
    }
}

} // namespace main
} // namespace kuzu
