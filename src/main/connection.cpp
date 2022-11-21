#include "main/connection.h"

#include "main/database.h"
#include "main/plan_printer.h"
#include "parser/parser.h"
#include "planner/planner.h"
#include "processor/mapper/plan_mapper.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

Connection::Connection(Database* database) {
    assert(database != nullptr);
    this->database = database;
    clientContext = make_unique<ClientContext>();
    transactionMode = AUTO_COMMIT;
}

Connection::~Connection() {
    if (activeTransaction) {
        database->transactionManager->rollback(activeTransaction.get());
    }
}

unique_ptr<QueryResult> Connection::query(const string& query) {
    lock_t lck{mtx};
    unique_ptr<PreparedStatement> preparedStatement;
    preparedStatement = prepareNoLock(query);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

unique_ptr<QueryResult> Connection::queryResultWithError(std::string& errMsg) {
    auto queryResult = make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

void Connection::setQuerySummaryAndPreparedStatement(
    Statement* statement, Binder& binder, PreparedStatement* preparedStatement) {
    switch (statement->getStatementType()) {
    case StatementType::QUERY: {
        auto parsedQuery = (RegularQuery*)statement;
        preparedStatement->preparedSummary.isExplain = parsedQuery->isEnableExplain();
        preparedStatement->preparedSummary.isProfile = parsedQuery->isEnableProfile();
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->allowActiveTransaction = true;
    } break;
    case StatementType::COPY_CSV:
    case StatementType::CREATE_REL_CLAUSE:
    case StatementType::CREATE_NODE_CLAUSE:
    case StatementType::DROP_TABLE: {
        preparedStatement->allowActiveTransaction = false;
    } break;
    default:
        assert(false);
    }
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(const string& query) {
    auto preparedStatement = make_unique<PreparedStatement>();
    if (query.empty()) {
        preparedStatement->success = false;
        preparedStatement->errMsg = "Connection Exception: Query is empty.";
        return preparedStatement;
    }
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    unique_ptr<ExecutionContext> executionContext;
    unique_ptr<LogicalPlan> logicalPlan;
    try {
        auto statement = Parser::parseQuery(query);
        auto binder = Binder(*database->catalog);
        auto boundStatement = binder.bind(*statement);
        setQuerySummaryAndPreparedStatement(statement.get(), binder, preparedStatement.get());
        // planning
        logicalPlan = Planner::getBestPlan(*database->catalog,
            database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs(),
            database->storageManager->getRelsStore().getRelsStatistics(), *boundStatement);
        if (logicalPlan->isDDLOrCopyCSV()) {
            preparedStatement->createResultHeader(
                expression_vector{make_shared<Expression>(LITERAL, DataType{STRING}, "outputMsg")});
        } else {
            preparedStatement->createResultHeader(logicalPlan->getExpressionsToCollect());
        }
    } catch (exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
    }
    compilingTimer.stop();
    preparedStatement->preparedSummary.compilingTime = compilingTimer.getElapsedTimeMS();
    preparedStatement->logicalPlan = std::move(logicalPlan);
    return preparedStatement;
}

string Connection::getNodeTableNames() {
    lock_t lck{mtx};
    string result = "Node tables: \n";
    for (auto& tableIDSchema : database->catalog->getReadOnlyVersion()->getNodeTableSchemas()) {
        result += "\t" + tableIDSchema.second->tableName + "\n";
    }
    return result;
}

string Connection::getRelTableNames() {
    lock_t lck{mtx};
    string result = "Rel tables: \n";
    for (auto& tableIDSchema : database->catalog->getReadOnlyVersion()->getRelTableSchemas()) {
        result += "\t" + tableIDSchema.second->tableName + "\n";
    }
    return result;
}

string Connection::getNodePropertyNames(const string& tableName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containNodeTable(tableName)) {
        throw Exception("Cannot find node table " + tableName);
    }
    string result = tableName + " properties: \n";
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName(tableName);
    auto primaryKeyPropertyID =
        catalog->getReadOnlyVersion()->getNodeTableSchema(tableID)->getPrimaryKey().propertyID;
    for (auto& property : catalog->getReadOnlyVersion()->getAllNodeProperties(tableID)) {
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType);
        result += property.propertyID == primaryKeyPropertyID ? "(PRIMARY KEY)\n" : "\n";
    }
    return result;
}

string Connection::getRelPropertyNames(const string& relTableName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containRelTable(relTableName)) {
        throw Exception("Cannot find rel table " + relTableName);
    }
    auto relTableID = catalog->getReadOnlyVersion()->getRelTableIDFromName(relTableName);
    string result = relTableName + " src nodes: \n";
    for (auto& nodeTableID :
        catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(relTableID, FWD)) {
        result += "\t" + catalog->getReadOnlyVersion()->getNodeTableName(nodeTableID) + "\n";
    }
    result += relTableName + " dst nodes: \n";
    for (auto& nodeTableID :
        catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(relTableID, BWD)) {
        result += "\t" + catalog->getReadOnlyVersion()->getNodeTableName(nodeTableID) + "\n";
    }
    result += relTableName + " properties: \n";
    for (auto& property : catalog->getReadOnlyVersion()->getRelProperties(relTableID)) {
        if (TableSchema::isReservedPropertyName(property.name)) {
            continue;
        }
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType) + "\n";
    }
    return result;
}

vector<unique_ptr<planner::LogicalPlan>> Connection::enumeratePlans(const string& query) {
    lock_t lck{mtx};
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery = Binder(*database->catalog).bind(*parsedQuery);
    return Planner::getAllPlans(*database->catalog,
        database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs(),
        database->storageManager->getRelsStore().getRelsStatistics(), *boundQuery);
}

unique_ptr<planner::LogicalPlan> Connection::getBestPlan(const std::string& query) {
    lock_t lck{mtx};
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery = Binder(*database->catalog).bind(*parsedQuery);
    return Planner::getBestPlan(*database->catalog,
        database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs(),
        database->storageManager->getRelsStore().getRelsStatistics(), *boundQuery);
}

unique_ptr<QueryResult> Connection::executePlan(unique_ptr<LogicalPlan> logicalPlan) {
    lock_t lck{mtx};
    auto preparedStatement = make_unique<PreparedStatement>();
    preparedStatement->createResultHeader(logicalPlan->getExpressionsToCollect());
    preparedStatement->logicalPlan = std::move(logicalPlan);
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
}

std::unique_ptr<QueryResult> Connection::executeWithParams(
    PreparedStatement* preparedStatement, unordered_map<string, shared_ptr<Literal>>& inputParams) {
    lock_t lck{mtx};
    if (!preparedStatement->isSuccess()) {
        return queryResultWithError(preparedStatement->errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, inputParams);
    } catch (Exception& exception) {
        string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
    return executeAndAutoCommitIfNecessaryNoLock(preparedStatement);
}

void Connection::bindParametersNoLock(
    PreparedStatement* preparedStatement, unordered_map<string, shared_ptr<Literal>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, literal] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw Exception("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        if (expectParam->dataType.typeID != literal->dataType.typeID) {
            throw Exception("Parameter " + name + " has data type " +
                            Types::dataTypeToString(literal->dataType) + " but expect " +
                            Types::dataTypeToString(expectParam->dataType) + ".");
        }
        parameterMap.at(name)->bind(*literal);
    }
}

std::unique_ptr<QueryResult> Connection::executeAndAutoCommitIfNecessaryNoLock(
    PreparedStatement* preparedStatement) {
    auto mapper = PlanMapper(
        *database->storageManager, database->memoryManager.get(), database->catalog.get());
    unique_ptr<PhysicalPlan> physicalPlan;
    if (preparedStatement->isSuccess()) {
        try {
            physicalPlan = mapper.mapLogicalPlanToPhysical(preparedStatement->logicalPlan.get());
        } catch (exception& exception) {
            preparedStatement->success = false;
            preparedStatement->errMsg = exception.what();
        }
    }
    if (!preparedStatement->isSuccess()) {
        rollbackIfNecessaryNoLock();
        return queryResultWithError(preparedStatement->errMsg);
    }
    auto queryResult = make_unique<QueryResult>(preparedStatement->preparedSummary);
    auto profiler = make_unique<Profiler>();
    auto executionContext = make_unique<ExecutionContext>(clientContext->numThreadsForExecution,
        profiler.get(), database->memoryManager.get(), database->bufferManager.get());
    // Execute query if EXPLAIN is not enabled.
    if (!preparedStatement->preparedSummary.isExplain) {
        profiler->enabled = preparedStatement->preparedSummary.isProfile;
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        shared_ptr<FactorizedTable> resultFT;
        try {
            beginTransactionIfAutoCommit(preparedStatement);
            executionContext->transaction = activeTransaction.get();
            resultFT =
                database->queryProcessor->execute(physicalPlan.get(), executionContext.get());
            if (AUTO_COMMIT == transactionMode) {
                commitNoLock();
            }
        } catch (Exception& exception) {
            rollbackIfNecessaryNoLock();
            string errMsg = exception.what();
            return queryResultWithError(errMsg);
        }
        executingTimer.stop();
        queryResult->querySummary->executionTime = executingTimer.getElapsedTimeMS();
        queryResult->setResultHeaderAndTable(
            preparedStatement->resultHeader->copy(), std::move(resultFT));
    }
    auto planPrinter = make_unique<PlanPrinter>(physicalPlan.get(), std::move(profiler));
    queryResult->querySummary->planInJson = planPrinter->printPlanToJson();
    queryResult->querySummary->planInOstream = planPrinter->printPlanToOstream();
    return queryResult;
}

void Connection::beginTransactionNoLock(TransactionType type) {
    if (activeTransaction) {
        throw ConnectionException(
            "Connection already has an active transaction. Applications can have one "
            "transaction per connection at any point in time. For concurrent multiple "
            "transactions, please open other connections. Current active transaction is not "
            "affected by this exception and can still be used.");
    }
    activeTransaction = type == READ_ONLY ?
                            database->transactionManager->beginReadOnlyTransaction() :
                            database->transactionManager->beginWriteTransaction();
}

void Connection::commitOrRollbackNoLock(bool isCommit, bool skipCheckpointForTesting) {
    if (activeTransaction) {
        if (activeTransaction->isWriteTransaction()) {
            database->commitAndCheckpointOrRollback(
                activeTransaction.get(), isCommit, skipCheckpointForTesting);
        } else {
            isCommit ? database->transactionManager->commit(activeTransaction.get()) :
                       database->transactionManager->rollback(activeTransaction.get());
        }
        activeTransaction.reset();
        transactionMode = AUTO_COMMIT;
    }
}

void Connection::beginTransactionIfAutoCommit(PreparedStatement* preparedStatement) {
    if (!preparedStatement->isReadOnly() && activeTransaction && activeTransaction->isReadOnly()) {
        throw ConnectionException("Can't execute a write query inside a read-only transaction.");
    }
    if (!preparedStatement->allowActiveTransaction && activeTransaction) {
        throw ConnectionException(
            "DDL and CopyCSV statements are automatically wrapped in a "
            "transaction and committed. As such, they cannot be part of an "
            "active transaction, please commit or rollback your previous transaction and "
            "issue a ddl query without opening a transaction.");
    }
    if (AUTO_COMMIT == transactionMode) {
        assert(!activeTransaction);
        // If the caller didn't explicitly start a transaction, we do so now and commit or
        // rollback here if necessary, i.e., if the given prepared statement has write
        // operations.
        beginTransactionNoLock(preparedStatement->isReadOnly() ? READ_ONLY : WRITE);
    }
    if (!activeTransaction) {
        assert(MANUAL == transactionMode);
        throw ConnectionException(
            "Transaction mode is manual but there is no active transaction. Please begin a "
            "transaction or set the transaction mode of the connection to AUTO_COMMIT");
    }
}

} // namespace main
} // namespace kuzu
