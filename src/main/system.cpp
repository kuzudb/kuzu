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

unique_ptr<ExecutionResult> System::executeQuery(SessionContext& sessionContext) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    sessionContext.profiler->reset();

    auto parsedQuery = Parser::parseQuery(sessionContext.query);
    sessionContext.profiler->enabled = parsedQuery->enable_profile;

    auto bindingTimeMetric = sessionContext.profiler->registerTimeMetric("binding");
    bindingTimeMetric->start();
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);
    bindingTimeMetric->stop();

    auto planningTimeMetric = sessionContext.profiler->registerTimeMetric("planning");
    planningTimeMetric->start();
    auto logicalPlan = Enumerator(*graph, *boundQuery).getBestPlan();
    planningTimeMetric->stop();

    auto executionContext =
        ExecutionContext(*sessionContext.profiler, sessionContext.activeTransaction);
    auto mappingTimeMetric = sessionContext.profiler->registerTimeMetric("mapping");
    mappingTimeMetric->start();
    auto physicalPlan = PlanMapper::mapToPhysical(move(logicalPlan), *graph, executionContext);
    mappingTimeMetric->stop();

    auto executingTimeMetric = sessionContext.profiler->registerTimeMetric("executing");
    executingTimeMetric->start();
    auto result = processor->execute(physicalPlan.get(), sessionContext.numThreads);
    executingTimeMetric->stop();

    return make_unique<ExecutionResult>(move(physicalPlan), move(result));
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

unique_ptr<ExecutionResult> System::executePlan(
    unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const {
    sessionContext.profiler->reset();
    auto executionContext =
        ExecutionContext(*sessionContext.profiler, sessionContext.activeTransaction);
    auto physicalPlan = PlanMapper::mapToPhysical(move(logicalPlan), *graph, executionContext);
    auto result = processor->execute(physicalPlan.get(), sessionContext.numThreads);
    return make_unique<ExecutionResult>(move(physicalPlan), move(result));
}

unique_ptr<nlohmann::json> System::debugInfo() const {
    auto json = make_unique<nlohmann::json>();
    (*json)["graph"] = *graph->debugInfo();
    return json;
}

} // namespace main
} // namespace graphflow
