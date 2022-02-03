#include "src/main/include/system.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"
#include "src/planner/include/planner.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"

using namespace graphflow::parser;
using namespace graphflow::binder;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

System::System(const string& path, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("System")} {
    memManager = make_unique<MemoryManager>();
    bufferManager =
        make_unique<BufferManager>(isInMemoryMode ? 0 : StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    graph = make_unique<Graph>(path, *bufferManager, isInMemoryMode);
    processor = make_unique<QueryProcessor>(thread::hardware_concurrency());
    initialized = true;
}

void System::executeQuery(SessionContext& context) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    context.clear();

    if (context.query.empty()) {
        logger->warn("Empty query input.");
        return;
    }

    auto parsedQuery = Parser::parseQuery(context.query);
    context.enable_explain = parsedQuery->isEnableExplain();
    context.profiler->enabled = parsedQuery->isEnableProfile();

    // compiling stage
    auto compilingTimeMetric = TimeMetric(true);
    compilingTimeMetric.start();
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);

    auto logicalPlan = Planner::getBestPlan(*graph, *boundQuery);
    if (logicalPlan->containAggregation() && context.numThreads > 1) {
        throw invalid_argument("Aggregation with multi-threading is not implemented.");
    }

    auto executionContext = make_unique<ExecutionContext>(*context.profiler, memManager.get());
    auto mapper = PlanMapper(*graph);
    auto physicalPlan = mapper.mapLogicalPlanToPhysical(move(logicalPlan), *executionContext);
    compilingTimeMetric.stop();
    context.compilingTime = compilingTimeMetric.getElapsedTimeMS();

    if (context.enable_explain) {
        context.planPrinter =
            make_unique<PlanPrinter>(move(physicalPlan), mapper.physicalIDToLogicalOperatorMap);
        return;
    }

    auto executingTimeMetric = TimeMetric(true);
    executingTimeMetric.start();
    auto result = processor->execute(physicalPlan.get(), context.numThreads);
    executingTimeMetric.stop();
    context.executingTime = executingTimeMetric.getElapsedTimeMS();

    context.planPrinter =
        make_unique<PlanPrinter>(move(physicalPlan), mapper.physicalIDToLogicalOperatorMap);
    context.queryResult = move(result);
}

vector<unique_ptr<LogicalPlan>> System::enumerateAllPlans(SessionContext& sessionContext) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    auto parsedQuery = Parser::parseQuery(sessionContext.query);
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);
    return Planner::getAllPlans(*graph, *boundQuery);
}

shared_ptr<FactorizedTable> System::executePlan(
    unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const {
    sessionContext.profiler->resetMetrics();
    auto executionContext = ExecutionContext(*sessionContext.profiler, memManager.get());
    auto physicalPlan =
        PlanMapper(*graph).mapLogicalPlanToPhysical(move(logicalPlan), executionContext);
    return processor->execute(physicalPlan.get(), sessionContext.numThreads);
}

unique_ptr<nlohmann::json> System::debugInfo() const {
    auto json = make_unique<nlohmann::json>();
    (*json)["graph"] = *graph->debugInfo();
    (*json)["bufferManager"] = *(bufferManager->debugInfo());
    return json;
}

} // namespace main
} // namespace graphflow
