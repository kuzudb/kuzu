#include "include/connection.h"

#include "include/database.h"
#include "include/plan_printer.h"

#include "src/binder/include/query_binder.h"
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
    if (isManualModeAndNoActiveTransactionNoLock()) {
        return queryResultWithErrorForNoActiveTransaction();
    }
    unique_ptr<PreparedStatement> preparedStatement;
    preparedStatement = prepareNoLock(query);
    if (preparedStatement->success) {
        return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
    }
    auto queryResult = make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = preparedStatement->errMsg;
    return queryResult;
}

unique_ptr<QueryResult> Connection::queryResultWithError(std::string& errMsg) {
    auto queryResult = make_unique<QueryResult>();
    queryResult->success = false;
    queryResult->errMsg = errMsg;
    return queryResult;
}

std::unique_ptr<PreparedStatement> Connection::prepareNoLock(const std::string& query) {
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
        // parsing
        auto parsedQuery = Parser::parseQuery(query);
        querySummary->isExplain = parsedQuery->isEnableExplain();
        querySummary->isProfile = parsedQuery->isEnableProfile();
        // binding
        auto binder = QueryBinder(*database->catalog);
        auto boundQuery = binder.bind(*parsedQuery);
        preparedStatement->parameterMap = binder.getParameterMap();
        // planning
        auto logicalPlan = Planner::getBestPlan(*database->catalog, *boundQuery);
        preparedStatement->createResultHeader(logicalPlan->getExpressionsToCollectDataTypes());
        // mapping
        auto mapper = PlanMapper(*database->catalog, *database->storageManager);
        executionContext = make_unique<ExecutionContext>(*preparedStatement->profiler,
            database->memoryManager.get(), database->bufferManager.get());
        physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *executionContext);
        preparedStatement->physicalIDToLogicalOperatorMap = mapper.physicalIDToLogicalOperatorMap;
    } catch (Exception& exception) {
        preparedStatement->success = false;
        preparedStatement->errMsg = exception.what();
    }
    compilingTimer.stop();
    querySummary->compilingTime = compilingTimer.getElapsedTimeMS();
    preparedStatement->executionContext = move(executionContext);
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
    for (auto i = 0u; i < database->catalog->getNumNodeLabels(); ++i) {
        result += "\t" + database->catalog->getNodeLabelName(i) + "\n";
    }
    return result;
}

string Connection::getRelLabelNames() {
    lock_t lck{mtx};
    string result = "Rel labels: \n";
    for (auto i = 0u; i < database->catalog->getNumRelLabels(); ++i) {
        result += "\t" + database->catalog->getRelLabelName(i) + "\n";
    }
    return result;
}

string Connection::getNodePropertyNames(const string& nodeLabelName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->containNodeLabel(nodeLabelName)) {
        throw Exception("Cannot find node label " + nodeLabelName);
    }
    string result = nodeLabelName + " properties: \n";
    auto labelId = catalog->getNodeLabelFromName(nodeLabelName);
    for (auto& property : catalog->getAllNodeProperties(labelId)) {
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType);
        result += property.isIDProperty() ? "(ID PROPERTY)\n" : "\n";
    }
    return result;
}

string Connection::getRelPropertyNames(const string& relLabelName) {
    lock_t lck{mtx};
    auto catalog = database->catalog.get();
    if (!catalog->containRelLabel(relLabelName)) {
        throw Exception("Cannot find rel label " + relLabelName);
    }
    auto labelId = catalog->getRelLabelFromName(relLabelName);
    string result = relLabelName + " src nodes: \n";
    for (auto& nodeLabelId : catalog->getNodeLabelsForRelLabelDirection(labelId, FWD)) {
        result += "\t" + catalog->getNodeLabelName(nodeLabelId) + "\n";
    }
    result += relLabelName + " dst nodes: \n";
    for (auto& nodeLabelId : catalog->getNodeLabelsForRelLabelDirection(labelId, BWD)) {
        result += "\t" + catalog->getNodeLabelName(nodeLabelId) + "\n";
    }
    result += relLabelName + " properties: \n";
    for (auto& property : catalog->getRelProperties(labelId)) {
        result += "\t" + property.name + " " + Types::dataTypeToString(property.dataType) + "\n";
    }
    return result;
}

vector<unique_ptr<planner::LogicalPlan>> Connection::enumeratePlans(const string& query) {
    lock_t lck{mtx};
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery = QueryBinder(*database->catalog).bind(*parsedQuery);
    return Planner::getAllPlans(*database->catalog, *boundQuery);
}

unique_ptr<QueryResult> Connection::executePlan(unique_ptr<LogicalPlan> logicalPlan) {
    lock_t lck{mtx};
    auto profiler = make_unique<Profiler>();
    profiler->resetMetrics();
    profiler->enabled = false;
    auto header = make_unique<QueryResultHeader>(logicalPlan->getExpressionsToCollectDataTypes());
    auto mapper = PlanMapper(*database->catalog, *database->storageManager);
    auto executionContext = make_unique<ExecutionContext>(
        *profiler, database->memoryManager.get(), database->bufferManager.get());
    auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *executionContext);
    auto table = database->queryProcessor->execute(
        physicalPlan.get(), clientContext->numThreadsForExecution);
    auto queryResult = make_unique<QueryResult>();
    queryResult->setResultHeaderAndTable(move(header), move(table));
    queryResult->querySummary =
        make_unique<QuerySummary>(); // TODO(Xiyang): remove this line after fixing iterator.
    return queryResult;
}

std::unique_ptr<QueryResult> Connection::executeWithParams(
    PreparedStatement* preparedStatement, unordered_map<string, shared_ptr<Literal>>& inputParams) {
    auto queryResult = make_unique<QueryResult>();
    if (!preparedStatement->isSuccess()) {
        auto errMsg = preparedStatement->getErrorMessage();
        return queryResultWithError(errMsg);
    }
    try {
        bindParametersNoLock(preparedStatement, inputParams);
    } catch (Exception& exception) {
        queryResult->success = false;
        queryResult->errMsg = exception.what();
    }
    if (queryResult->success) {
        return executeLock(preparedStatement);
    }
    return queryResult;
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
    unique_ptr<QueryResult> queryResult;
    auto querySummary = preparedStatement->querySummary.get();
    // executing if not EXPLAIN
    if (!querySummary->isExplain) {
        preparedStatement->profiler->enabled = querySummary->isProfile;
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        shared_ptr<FactorizedTable> table;
        try {
            if (AUTO_COMMIT == transactionMode) {
                assert(!activeTransaction);
                // If the caller didn't explicitly start a transaction, we do so now and commit or
                // rollback here if necessary, i.e., if the given prepared statement has write
                // operations.
                beginTransactionNoLock(preparedStatement->isReadOnly() ? READ_ONLY : WRITE);
            }
            table = database->queryProcessor->execute(
                preparedStatement->physicalPlan.get(), clientContext->numThreadsForExecution);
            if (AUTO_COMMIT == transactionMode) {
                commitNoLock();
            }
            queryResult = make_unique<QueryResult>();
        } catch (Exception& exception) {
            string errMsg = exception.what();
            queryResult = queryResultWithError(errMsg);
            rollbackNoLock();
        }
        executingTimer.stop();
        querySummary->executionTime = executingTimer.getElapsedTimeMS();
        if (queryResult->success) {
            queryResult->setResultHeaderAndTable(
                move(preparedStatement->resultHeader), move(table));
            preparedStatement->createPlanPrinter();
            queryResult->querySummary = move(preparedStatement->querySummary);
        }
        return queryResult;
    }
    // create printable plan and return if EXPLAIN
    queryResult = make_unique<QueryResult>();
    preparedStatement->createPlanPrinter();
    queryResult->querySummary = move(preparedStatement->querySummary);
    return queryResult;
}

void Connection::beginTransactionNoLock(TransactionType type) {
    if (activeTransaction) {
        throw ConnectionException(
            "Connection already has an active transaction. Applications can have"
            " one transaction per connection at any point in time. For concurrent multiple"
            " transactions, please open other connections. Current active transaction is not"
            " affected by this exception and can still be used.");
    }
    activeTransaction =
        (type == READ_ONLY ? database->transactionManager->beginReadOnlyTransaction() :
                             database->transactionManager->beginWriteTransaction());
}

void Connection::commitOrRollbackNoLock(bool isCommit) {
    if (activeTransaction) {
        isCommit ? database->transactionManager->commit(activeTransaction.get()) :
                   database->transactionManager->rollback(activeTransaction.get());
        activeTransaction.reset();
        transactionMode = AUTO_COMMIT;
    }
}

} // namespace main
} // namespace graphflow
