#include "src/main/include/system.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"
#include "src/planner/include/enumerator.h"
#include "src/processor/include/physical_plan/plan_mapper.h"

using namespace graphflow::parser;
using namespace graphflow::binder;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

System::System(const string& path) {
    graph = make_unique<Graph>(path);
    processor = make_unique<QueryProcessor>(thread::hardware_concurrency());
    transactionManager = make_unique<TransactionManager>();
    initialized = true;
}

unique_ptr<QueryResult> System::executeQuery(SessionContext& sessionContext) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    sessionContext.profiler.reset();

    auto parsedQuery = Parser::parseQuery(sessionContext.query);
    sessionContext.profiler.enable = parsedQuery->enable_profile;

    auto& profiler = sessionContext.profiler;

    auto bindingTimeMetric = profiler.registerPhaseTimeMetric("Binding");
    bindingTimeMetric->start();
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);
    bindingTimeMetric->stop();

    auto planningTimeMetric = profiler.registerPhaseTimeMetric("Planning");
    planningTimeMetric->start();
    auto logicalPlan = Enumerator(*graph, *boundQuery).getBestPlan();
    planningTimeMetric->stop();

    auto mappingTimeMetric = profiler.registerPhaseTimeMetric("Mapping");
    mappingTimeMetric->start();
    auto physicalPlan =
        PlanMapper::mapToPhysical(move(logicalPlan), sessionContext.activeTransaction, *graph);
    mappingTimeMetric->stop();

    auto executingTimeMetric = profiler.registerPhaseTimeMetric("Executing");
    auto executionContext = ExecutionContext(move(physicalPlan), sessionContext.numThreads);
    executingTimeMetric->start();
    auto result = processor->execute(executionContext);
    executingTimeMetric->stop();

    return result;
}

vector<unique_ptr<LogicalPlan>> System::enumerateAllPlans(SessionContext& sessionContext) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    auto parsedQuery = Parser::parseQuery(sessionContext.query);
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);
    auto logicalPlans = Enumerator(*graph, *boundQuery).enumeratePlans();
    return logicalPlans;
}

unique_ptr<QueryResult> System::executePlan(
    unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const {
    auto physicalPlan =
        PlanMapper::mapToPhysical(move(logicalPlan), sessionContext.activeTransaction, *graph);
    auto executionContext = ExecutionContext(move(physicalPlan), sessionContext.numThreads);
    auto result = processor->execute(executionContext);
    return result;
}

unique_ptr<nlohmann::json> System::debugInfo() const {
    auto json = make_unique<nlohmann::json>();
    (*json)["graph"] = *graph->debugInfo();
    return json;
}

} // namespace main
} // namespace graphflow
