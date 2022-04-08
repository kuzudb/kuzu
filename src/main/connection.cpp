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

namespace graphflow {
namespace main {

Connection::Connection(Database* database) {
    assert(database != nullptr);
    this->database = database;
    clientContext = make_unique<ClientContext>();
}

unique_ptr<QueryResult> Connection::query(const string& query) {
    auto preparedStatement = prepare(query);
    return execute(preparedStatement.get());
}

std::unique_ptr<PreparedStatement> Connection::prepare(const std::string& query) {
    auto preparedStatement = make_unique<PreparedStatement>();
    if (query.empty()) { // TODO: replace this with a failure state
        throw invalid_argument("input query cannot be empty");
    }
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    auto querySummary = preparedStatement->querySummary.get();
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
    preparedStatement->physicalIDToLogicalOperatorMap = mapper.physicalIDToLogicalOperatorMap;
    auto executionContext = make_unique<ExecutionContext>(
        *preparedStatement->profiler, database->memoryManager.get(), database->bufferManager.get());
    auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *executionContext);
    compilingTimer.stop();
    querySummary->compilingTime = compilingTimer.getElapsedTimeMS();
    preparedStatement->executionContext = move(executionContext);
    preparedStatement->physicalPlan = move(physicalPlan);
    return preparedStatement;
}

vector<unique_ptr<planner::LogicalPlan>> Connection::enumeratePlans(const string& query) {
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery = QueryBinder(*database->catalog).bind(*parsedQuery);
    return Planner::getAllPlans(*database->catalog, *boundQuery);
}

unique_ptr<QueryResult> Connection::executePlan(unique_ptr<LogicalPlan> logicalPlan) {
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
    return make_unique<QueryResult>(
        move(header), move(table), make_unique<QuerySummary>() /* querySummary */);
}

std::unique_ptr<QueryResult> Connection::executeWithParams(
    PreparedStatement* preparedStatement, unordered_map<string, shared_ptr<Literal>>& inputParams) {
    bindParameters(preparedStatement, inputParams);
    return execute(preparedStatement);
}

void Connection::bindParameters(
    PreparedStatement* preparedStatement, unordered_map<string, shared_ptr<Literal>>& inputParams) {
    auto& parameterMap = preparedStatement->parameterMap;
    for (auto& [name, literal] : inputParams) {
        if (!parameterMap.contains(name)) {
            throw invalid_argument("Parameter " + name + " not found.");
        }
        auto expectParam = parameterMap.at(name);
        if (expectParam->dataType.typeID != literal->dataType.typeID) {
            throw invalid_argument("Parameter " + name + " has data type " +
                                   Types::dataTypeToString(literal->dataType) + " but expect " +
                                   Types::dataTypeToString(expectParam->dataType) + ".");
        }
        parameterMap.at(name)->bind(*literal);
    }
}

std::unique_ptr<QueryResult> Connection::execute(PreparedStatement* preparedStatement) {
    auto lck = acquireLock();
    // executing if not EXPLAIN
    if (!preparedStatement->querySummary->isExplain) {
        preparedStatement->profiler->enabled = preparedStatement->querySummary->isProfile;
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        auto table = database->queryProcessor->execute(
            preparedStatement->physicalPlan.get(), clientContext->numThreadsForExecution);
        executingTimer.stop();
        preparedStatement->querySummary->executionTime = executingTimer.getElapsedTimeMS();
        preparedStatement->createPlanPrinter();
        return make_unique<QueryResult>(move(preparedStatement->resultHeader), move(table),
            move(preparedStatement->querySummary));
    }
    preparedStatement->createPlanPrinter();
    return make_unique<QueryResult>(move(preparedStatement->querySummary));
}

} // namespace main
} // namespace graphflow
