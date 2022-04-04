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
    auto lck = acquireLock();
    auto querySummary = make_unique<QuerySummary>();
    if (query.empty()) { // TODO: replace this with a failure state
        throw invalid_argument("input query cannot be empty");
    }
    auto compilingTimer = TimeMetric(true /* enable */);
    compilingTimer.start();
    // parsing
    auto parsedQuery = Parser::parseQuery(query);
    querySummary->isExplain = parsedQuery->isEnableExplain();
    querySummary->isProfile = parsedQuery->isEnableProfile();
    // binding
    auto boundQuery = QueryBinder(*database->catalog).bind(*parsedQuery);
    // planning
    auto logicalPlan = Planner::getBestPlan(*database->catalog, *boundQuery);
    auto header = make_unique<QueryResultHeader>(logicalPlan->getExpressionsToCollectDataTypes());
    // mapping
    auto mapper = PlanMapper(*database->catalog, *database->storageManager);
    auto profiler = make_unique<Profiler>();
    auto executionContext = make_unique<ExecutionContext>(
        *profiler, database->memoryManager.get(), database->bufferManager.get());
    auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *executionContext);
    compilingTimer.stop();
    querySummary->compilingTime = compilingTimer.getElapsedTimeMS();
    // executing if not EXPLAIN
    if (!parsedQuery->isEnableExplain()) {
        configProfiler(*profiler, parsedQuery->isEnableProfile());
        auto executingTimer = TimeMetric(true /* enable */);
        executingTimer.start();
        auto table = database->queryProcessor->execute(
            physicalPlan.get(), clientContext->numThreadsForExecution);
        executingTimer.stop();
        querySummary->executionTime = executingTimer.getElapsedTimeMS();
        querySummary->planPrinter = make_unique<PlanPrinter>(
            move(physicalPlan), mapper.physicalIDToLogicalOperatorMap, move(profiler));
        return make_unique<QueryResult>(move(header), move(table), move(querySummary));
    }
    querySummary->planPrinter = make_unique<PlanPrinter>(
        move(physicalPlan), mapper.physicalIDToLogicalOperatorMap, move(profiler));
    return make_unique<QueryResult>(move(querySummary));
}

vector<unique_ptr<planner::LogicalPlan>> Connection::enumeratePlans(const string& query) {
    auto parsedQuery = Parser::parseQuery(query);
    auto boundQuery = QueryBinder(*database->catalog).bind(*parsedQuery);
    return Planner::getAllPlans(*database->catalog, *boundQuery);
}

unique_ptr<QueryResult> Connection::executePlan(unique_ptr<LogicalPlan> logicalPlan) {
    auto profiler = make_unique<Profiler>();
    configProfiler(*profiler, false /* isEnabled */);
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

void Connection::configProfiler(Profiler& profiler, bool isEnabled) {
    profiler.resetMetrics();
    if (isEnabled) {
        profiler.enabled = true;
    } else {
        profiler.enabled = false;
    }
}

} // namespace main
} // namespace graphflow
