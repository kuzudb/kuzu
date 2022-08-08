#include "include/connection.h"

#include "include/database.h"
#include "include/plan_printer.h"

#include "src/parser/include/parser.h"
#include "src/planner/include/planner.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"

using namespace std;
using namespace graphflow::parser;
using namespace graphflow::binder;
using namespace graphflow::planner;
using namespace graphflow::processor;
using namespace graphflow::transaction;

namespace graphflow {
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
    if (preparedStatement->success) {
        return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
    }
    return queryResultWithError(preparedStatement->errMsg);
}

unique_ptr<QueryResult> Connection::queryResultWithError(std::string& errMsg) {
    auto queryResult = make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

void Connection::setQuerySummaryAndPreparedStatement(Statement* statement, Binder& binder,
    QuerySummary* querySummary, PreparedStatement* preparedStatement) {
    switch (statement->getStatementType()) {
    case StatementType::QUERY: {
        auto parsedQuery = (RegularQuery*)statement;
        querySummary->isExplain = parsedQuery->isEnableExplain();
        querySummary->isProfile = parsedQuery->isEnableProfile();
        preparedStatement->parameterMap = binder.getParameterMap();
        preparedStatement->isDDL = false;
    } break;
    case StatementType::COPY_CSV: {
        preparedStatement->isDDL = false;
    } break;
    case StatementType::CREATE_REL_CLAUSE:
    case StatementType::CREATE_NODE_CLAUSE: {
        preparedStatement->isDDL = true;
    } break;
    default:
        assert(false);
    }
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(const string& query) {
    auto preparedStatement = make_unique<PreparedStatement>();
    if (query.empty()) {
        throw Exception("Input query is empty.");
    }
    auto querySummary = preparedStatement->querySummary.get();
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    unique_ptr<ExecutionContext> executionContext;
    unique_ptr<PhysicalPlan> physicalPlan;
    try {
        auto statement = Parser::parseQuery(query);
        auto binder = Binder(*database->catalog);
        auto boundStatement = binder.bind(*statement);
        setQuerySummaryAndPreparedStatement(
            statement.get(), binder, querySummary, preparedStatement.get());
        // planning
        auto logicalPlan = Planner::getBestPlan(*database->catalog,
            database->storageManager->getNodesStore().getNodesMetadata(), *boundStatement);
        preparedStatement->createResultHeader(logicalPlan->getExpressionsToCollect());
        // mapping
        auto mapper = PlanMapper(*database->storageManager, database->getMemoryManager(),
            database->catalog.get(), database->bufferManager.get());
        physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan));
    } catch (Exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
    }
    compilingTimer.stop();
    querySummary->compilingTime = compilingTimer.getElapsedTimeMS();
    preparedStatement->physicalPlan = move(physicalPlan);
    return preparedStatement;
}

string Connection::getBuiltInScalarFunctionNames() {
    lock_t lck{mtx};
    string result = "Built-in scalar functions: \n";
    for (auto& functionName : database->catalog->getBuiltInScalarFunctions()->getFunctionNames()) {
        result += functionName + "\n";
    }
    return result;
}

string Connection::getBuiltInAggregateFunctionNames() {
    lock_t lck{mtx};
    string result = "Built-in aggregate functions: \n";
    for (auto& functionName :
        database->catalog->getBuiltInAggregateFunction()->getFunctionNames()) {
        result += functionName + "\n";
    }
    return result;
}

string Connection::getNodeLabelNames() {
    lock_t lck{mtx};
    string result = "Node labels: \n";
    for (auto i = 0u; i < database->catalog->getReadOnlyVersion()->getNumNodeLabels(); ++i) {
        result += "\t" + database->catalog->getReadOnlyVersion()->getNodeLabelName(i) + "\n";
    }
    return result;
}

string Connection::getRelLabelNames() {
    lock_t lck{mtx};
    string result = "Rel labels: \n";
    for (auto i = 0u; i < database->catalog->getReadOnlyVersion()->getNumRelLabels(); ++i) {
        result += "\t" + database->catalog->getReadOnlyVersion()->getRelLabelName(i) + "\n";
    }
    return result;
}

string Connection::getNodePropertyNames(const string& nodeLabelName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containNodeLabel(nodeLabelName)) {
        throw Exception("Cannot find node label " + nodeLabelName);
    }
    string result = nodeLabelName + " properties: \n";
    auto labelId = catalog->getReadOnlyVersion()->getNodeLabelFromName(nodeLabelName);
    for (auto& property : catalog->getReadOnlyVersion()->getAllNodeProperties(labelId)) {
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType);
        result += property.isIDProperty() ? "(ID PROPERTY)\n" : "\n";
    }
    return result;
}

string Connection::getRelPropertyNames(const string& relLabelName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->getReadOnlyVersion()->containRelLabel(relLabelName)) {
        throw Exception("Cannot find rel label " + relLabelName);
    }
    auto labelId = catalog->getReadOnlyVersion()->getRelLabelFromName(relLabelName);
    string result = relLabelName + " src nodes: \n";
    for (auto& nodeLabelId :
        catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(labelId, FWD)) {
        result += "\t" + catalog->getReadOnlyVersion()->getNodeLabelName(nodeLabelId) + "\n";
    }
    result += relLabelName + " dst nodes: \n";
    for (auto& nodeLabelId :
        catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(labelId, BWD)) {
        result += "\t" + catalog->getReadOnlyVersion()->getNodeLabelName(nodeLabelId) + "\n";
    }
    result += relLabelName + " properties: \n";
    for (auto& property : catalog->getReadOnlyVersion()->getRelProperties(labelId)) {
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType) + "\n";
    }
    return result;
}

vector<unique_ptr<planner::LogicalPlan>> Connection::enumeratePlans(const string& query) {
    lock_t lck{mtx};
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery =
        Binder(*database->catalog).bind(*reinterpret_cast<RegularQuery*>(parsedQuery.get()));
    return Planner::getAllPlans(*database->catalog,
        database->storageManager->getNodesStore().getNodesMetadata(), *boundQuery);
}

unique_ptr<QueryResult> Connection::executePlan(unique_ptr<LogicalPlan> logicalPlan) {
    lock_t lck{mtx};
    auto preparedStatement = make_unique<PreparedStatement>();
    preparedStatement->createResultHeader(logicalPlan->getExpressionsToCollect());
    auto mapper = PlanMapper(*database->storageManager, database->getMemoryManager(),
        database->getCatalog(), database->getBufferManager());
    auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan));
    preparedStatement->physicalPlan = move(physicalPlan);
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
    auto queryResult = make_unique<QueryResult>();
    auto querySummary = preparedStatement->querySummary.get();
    auto profiler = make_unique<Profiler>();
    auto executionContext = make_unique<ExecutionContext>(clientContext->numThreadsForExecution,
        profiler.get(), database->memoryManager.get(), database->bufferManager.get());
    // executing if not EXPLAIN
    if (!querySummary->isExplain) {
        profiler->enabled = querySummary->isProfile;
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        shared_ptr<FactorizedTable> table;
        try {
            if (preparedStatement->isDDL && activeTransaction) {
                throw ConnectionException(
                    "DDL statements are automatically wrapped in a "
                    "transaction and committed automatically. As such, they cannot be part of an "
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
            executionContext->transaction = activeTransaction.get();
            table = database->queryProcessor->execute(
                preparedStatement->physicalPlan.get(), executionContext.get());
            if (AUTO_COMMIT == transactionMode) {
                commitNoLock();
            }
        } catch (Exception& exception) {
            rollbackNoLock();
            string errMsg = exception.what();
            return queryResultWithError(errMsg);
        }
        executingTimer.stop();
        querySummary->executionTime = executingTimer.getElapsedTimeMS();
        queryResult->setResultHeaderAndTable(move(preparedStatement->resultHeader), move(table));
        preparedStatement->createPlanPrinter(move(profiler));
        queryResult->querySummary = move(preparedStatement->querySummary);
        return queryResult;
    }
    // NOTE: If EXPLAIN is enabled, we still need to init physical plan to register profiler because
    // our plan printer will try to read from profiler metrics regardless whether it's enabled or
    // not. A better solution could be that we don't print any metric during EXPLAIN.
    preparedStatement->physicalPlan->lastOperator->init(executionContext.get());
    preparedStatement->createPlanPrinter(move(profiler));
    queryResult->querySummary = move(preparedStatement->querySummary);
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

} // namespace main
} // namespace graphflow
