#include "main/connection.h"

#include "binder/binder.h"
#include "binder/visitor/statement_read_write_analyzer.h"
#include "json.hpp"
#include "main/database.h"
#include "main/plan_printer.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/processor.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

Connection::Connection(Database* database) {
    assert(database != nullptr);
    this->database = database;
    clientContext = std::make_unique<ClientContext>();
    transactionMode = ConnectionTransactionMode::AUTO_COMMIT;
}

Connection::~Connection() {
    if (activeTransaction) {
        database->transactionManager->rollback(activeTransaction.get());
    }
}

void Connection::beginReadOnlyTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    setTransactionModeNoLock(ConnectionTransactionMode::MANUAL);
    beginTransactionNoLock(transaction::TransactionType::READ_ONLY);
}

void Connection::beginWriteTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    setTransactionModeNoLock(ConnectionTransactionMode::MANUAL);
    beginTransactionNoLock(transaction::TransactionType::WRITE);
}

void Connection::commit() {
    std::unique_lock<std::mutex> lck{mtx};
    commitOrRollbackNoLock(TransactionAction::COMMIT);
}

void Connection::rollback() {
    std::unique_lock<std::mutex> lck{mtx};
    commitOrRollbackNoLock(TransactionAction::ROLLBACK);
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

std::unique_ptr<QueryResult> Connection::queryResultWithError(std::string& errMsg) {
    auto queryResult = std::make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

Connection::ConnectionTransactionMode Connection::getTransactionMode() {
    std::unique_lock<std::mutex> lck{mtx};
    return transactionMode;
}

void Connection::setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode) {
    if (activeTransaction && transactionMode == ConnectionTransactionMode::MANUAL &&
        newTransactionMode == ConnectionTransactionMode::AUTO_COMMIT) {
        throw common::ConnectionException(
            "Cannot change transaction mode from MANUAL to AUTO_COMMIT when there is an "
            "active transaction. Need to first commit or rollback the active transaction.");
    }
    transactionMode = newTransactionMode;
}

void Connection::commitButSkipCheckpointingForTestingRecovery() {
    std::unique_lock<std::mutex> lck{mtx};
    commitOrRollbackNoLock(TransactionAction::COMMIT, true /* skip checkpointing for testing */);
}

void Connection::rollbackButSkipCheckpointingForTestingRecovery() {
    std::unique_lock<std::mutex> lck{mtx};
    commitOrRollbackNoLock(TransactionAction::ROLLBACK, true /* skip checkpointing for testing */);
}

transaction::Transaction* Connection::getActiveTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    return activeTransaction.get();
}

uint64_t Connection::getActiveTransactionID() {
    std::unique_lock<std::mutex> lck{mtx};
    return activeTransaction ? activeTransaction->getID() : UINT64_MAX;
}

bool Connection::hasActiveTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    return activeTransaction != nullptr;
}

void Connection::commitNoLock() {
    commitOrRollbackNoLock(TransactionAction::COMMIT);
}

void Connection::rollbackIfNecessaryNoLock() {
    // If there is no active transaction in the system (e.g., an exception occurs during
    // planning), we shouldn't roll back the transaction.
    if (activeTransaction != nullptr) {
        commitOrRollbackNoLock(TransactionAction::ROLLBACK);
    }
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
        preparedStatement->preparedSummary.isExplain = statement->isExplain();
        preparedStatement->preparedSummary.isProfile = statement->isProfile();
        // binding
        auto binder = Binder(*database->catalog);
        auto boundStatement = binder.bind(*statement);
        preparedStatement->statementType = boundStatement->getStatementType();
        preparedStatement->readOnly =
            binder::StatementReadWriteAnalyzer().isReadOnly(*boundStatement);
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->statementResult = boundStatement->getStatementResult()->copy();
        // planning
        auto& nodeStatistics =
            database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs();
        auto& relStatistics = database->storageManager->getRelsStore().getRelsStatistics();
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        if (enumerateAllPlans) {
            plans = Planner::getAllPlans(
                *database->catalog, nodeStatistics, relStatistics, *boundStatement);
        } else {
            plans.push_back(Planner::getBestPlan(
                *database->catalog, nodeStatistics, relStatistics, *boundStatement));
        }
        // optimizing
        for (auto& plan : plans) {
            optimizer::Optimizer::optimize(plan.get());
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

std::string Connection::getNodeTableNames() {
    lock_t lck{mtx};
    std::string result = "Node tables: \n";
    std::vector<std::string> nodeTableNames;
    for (auto& tableIDSchema : database->catalog->getReadOnlyVersion()->getNodeTableSchemas()) {
        nodeTableNames.push_back(tableIDSchema.second->tableName);
    }
    std::sort(nodeTableNames.begin(), nodeTableNames.end());
    for (auto& nodeTableName : nodeTableNames) {
        result += "\t" + nodeTableName + "\n";
    }
    return result;
}

std::string Connection::getRelTableNames() {
    lock_t lck{mtx};
    std::string result = "Rel tables: \n";
    std::vector<std::string> relTableNames;
    for (auto& tableIDSchema : database->catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        relTableNames.push_back(tableIDSchema.second->tableName);
    }
    std::sort(relTableNames.begin(), relTableNames.end());
    for (auto& relTableName : relTableNames) {
        result += "\t" + relTableName + "\n";
    }
    return result;
}

std::string Connection::getNodePropertyNames(const std::string& tableName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containNodeTable(tableName)) {
        throw RuntimeException("Cannot find node table " + tableName);
    }
    std::string result = tableName + " properties: \n";
    auto tableID = catalog->getReadOnlyVersion()->getTableID(tableName);
    auto primaryKeyPropertyID =
        catalog->getReadOnlyVersion()->getNodeTableSchema(tableID)->getPrimaryKey().propertyID;
    for (auto& property : catalog->getReadOnlyVersion()->getAllNodeProperties(tableID)) {
        result +=
            "\t" + property.name + " " + LogicalTypeUtils::dataTypeToString(property.dataType);
        result += property.propertyID == primaryKeyPropertyID ? "(PRIMARY KEY)\n" : "\n";
    }
    return result;
}

std::string Connection::getRelPropertyNames(const std::string& relTableName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containRelTable(relTableName)) {
        throw RuntimeException("Cannot find rel table " + relTableName);
    }
    auto relTableID = catalog->getReadOnlyVersion()->getTableID(relTableName);
    auto srcTableID =
        catalog->getReadOnlyVersion()->getRelTableSchema(relTableID)->getBoundTableID(FWD);
    auto srcTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(srcTableID);
    auto dstTableID =
        catalog->getReadOnlyVersion()->getRelTableSchema(relTableID)->getBoundTableID(BWD);
    auto dstTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(dstTableID);
    std::string result = relTableName + " src node: " + srcTableSchema->tableName + "\n";
    result += relTableName + " dst node: " + dstTableSchema->tableName + "\n";
    result += relTableName + " properties: \n";
    for (auto& property : catalog->getReadOnlyVersion()->getRelProperties(relTableID)) {
        if (catalog::TableSchema::isReservedPropertyName(property.name)) {
            continue;
        }
        result += "\t" + property.name + " " +
                  LogicalTypeUtils::dataTypeToString(property.dataType) + "\n";
    }
    return result;
}

void Connection::interrupt() {
    clientContext->activeQuery->interrupted = true;
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
    std::unordered_map<std::string, std::shared_ptr<Value>>& inputParams) {
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
    std::unordered_map<std::string, std::shared_ptr<Value>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, value] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw Exception("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        if (expectParam->dataType != value->getDataType()) {
            throw Exception("Parameter " + name + " has data type " +
                            LogicalTypeUtils::dataTypeToString(value->getDataType()) +
                            " but expects " +
                            LogicalTypeUtils::dataTypeToString(expectParam->dataType) + ".");
        }
        parameterMap.at(name)->copyValueFrom(*value);
    }
}

std::unique_ptr<QueryResult> Connection::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement, uint32_t planIdx) {
    clientContext->activeQuery = std::make_unique<ActiveQuery>();
    clientContext->startTimingIfEnabled();
    auto mapper = PlanMapper(
        *database->storageManager, database->memoryManager.get(), database->catalog.get());
    std::unique_ptr<PhysicalPlan> physicalPlan;
    if (preparedStatement->isSuccess()) {
        try {
            physicalPlan =
                mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlans[planIdx].get(),
                    preparedStatement->getExpressionsToCollect());
        } catch (std::exception& exception) {
            preparedStatement->success = false;
            preparedStatement->errMsg = exception.what();
        }
    }
    if (!preparedStatement->isSuccess()) {
        rollbackIfNecessaryNoLock();
        return queryResultWithError(preparedStatement->errMsg);
    }
    auto queryResult = std::make_unique<QueryResult>(preparedStatement->preparedSummary);
    auto profiler = std::make_unique<Profiler>();
    auto executionContext =
        std::make_unique<ExecutionContext>(clientContext->numThreadsForExecution, profiler.get(),
            database->memoryManager.get(), database->bufferManager.get(), clientContext.get());
    // Execute query if EXPLAIN is not enabled.
    if (!preparedStatement->preparedSummary.isExplain) {
        profiler->enabled = preparedStatement->preparedSummary.isProfile;
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        std::shared_ptr<FactorizedTable> resultFT;
        try {
            beginTransactionIfAutoCommit(preparedStatement);
            executionContext->transaction = activeTransaction.get();
            resultFT =
                database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
            if (ConnectionTransactionMode::AUTO_COMMIT == transactionMode) {
                commitNoLock();
            }
        } catch (Exception& exception) { return getQueryResultWithError(exception.what()); }
        executingTimer.stop();
        queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
        queryResult->initResultTableAndIterator(std::move(resultFT),
            preparedStatement->statementResult->getColumns(),
            preparedStatement->statementResult->getExpressionsToCollectPerColumn());
    }
    auto planPrinter = std::make_unique<PlanPrinter>(physicalPlan.get(), std::move(profiler));
    queryResult->querySummary->planInJson =
        std::make_unique<nlohmann::json>(planPrinter->printPlanToJson());
    queryResult->querySummary->planInOstream = planPrinter->printPlanToOstream();
    return queryResult;
}

void Connection::beginTransactionNoLock(TransactionType type) {
    if (activeTransaction) {
        throw ConnectionException(
            "Connection already has an active transaction. Applications can have one "
            "transaction per connection at any point in time. For concurrent multiple "
            "transactions, please open other connections. Current active transaction is "
            "not affected by this exception and can still be used.");
    }
    activeTransaction = type == transaction::TransactionType::READ_ONLY ?
                            database->transactionManager->beginReadOnlyTransaction() :
                            database->transactionManager->beginWriteTransaction();
}

void Connection::commitOrRollbackNoLock(
    transaction::TransactionAction action, bool skipCheckpointForTesting) {
    if (activeTransaction) {
        if (action == TransactionAction::COMMIT) {
            database->commit(activeTransaction.get(), skipCheckpointForTesting);
        } else {
            assert(action == TransactionAction::ROLLBACK);
            database->rollback(activeTransaction.get(), skipCheckpointForTesting);
        }
        activeTransaction.reset();
        transactionMode = ConnectionTransactionMode::AUTO_COMMIT;
    }
}

void Connection::beginTransactionIfAutoCommit(PreparedStatement* preparedStatement) {
    if (!preparedStatement->isReadOnly() && activeTransaction && activeTransaction->isReadOnly()) {
        throw ConnectionException("Can't execute a write query inside a read-only transaction.");
    }
    if (!preparedStatement->allowActiveTransaction() && activeTransaction) {
        throw ConnectionException(
            "DDL and CopyCSV statements are automatically wrapped in a "
            "transaction and committed. As such, they cannot be part of an "
            "active transaction, please commit or rollback your previous transaction and "
            "issue a ddl query without opening a transaction.");
    }
    if (ConnectionTransactionMode::AUTO_COMMIT == transactionMode) {
        assert(!activeTransaction);
        // If the caller didn't explicitly start a transaction, we do so now and commit or
        // rollback here if necessary, i.e., if the given prepared statement has write
        // operations.
        beginTransactionNoLock(preparedStatement->isReadOnly() ?
                                   transaction::TransactionType::READ_ONLY :
                                   transaction::TransactionType::WRITE);
    }
    if (!activeTransaction) {
        assert(ConnectionTransactionMode::MANUAL == transactionMode);
        throw ConnectionException(
            "Transaction mode is manual but there is no active transaction. Please begin a "
            "transaction or set the transaction mode of the connection to AUTO_COMMIT");
    }
}

} // namespace main
} // namespace kuzu
