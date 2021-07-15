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

System::System(const string& path, bool isInMemoryMode) {
    memManager = make_unique<MemoryManager>();
    bufferManager =
        make_unique<BufferManager>(isInMemoryMode ? 0 : StorageConfig::DEFAULT_BUFFER_POOL_SIZE);
    graph = make_unique<Graph>(path, *bufferManager, isInMemoryMode);
    processor = make_unique<QueryProcessor>(thread::hardware_concurrency());
    transactionManager = make_unique<TransactionManager>();
    initialized = true;
}

void System::executeQuery(SessionContext& context) const {
    if (!initialized) {
        throw invalid_argument("System is not initialized");
    }
    context.clear();

    auto parsedQuery = Parser::parseQuery(context.query);
    context.enable_explain = parsedQuery->enable_explain;
    context.profiler->enabled = parsedQuery->enable_profile;

    // compiling stage
    auto compilingTimeMetric = TimeMetric(true);
    compilingTimeMetric.start();
    auto boundQuery = QueryBinder(graph->getCatalog()).bind(*parsedQuery);

    auto logicalPlan = Enumerator(*graph, *boundQuery).getBestPlan();

    auto executionContext = make_unique<ExecutionContext>(
        *context.profiler, context.activeTransaction, memManager.get());
    auto mapper = PlanMapper(*graph);
    auto physicalPlan = mapper.mapToPhysical(move(logicalPlan), *executionContext);
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
    auto logicalPlans = Enumerator(*graph, *boundQuery).enumeratePlans();
    return logicalPlans;
}

unique_ptr<QueryResult> System::executePlan(
    unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const {
    sessionContext.profiler->resetMetrics();
    auto executionContext = ExecutionContext(
        *sessionContext.profiler, sessionContext.activeTransaction, memManager.get());
    auto physicalPlan = PlanMapper(*graph).mapToPhysical(move(logicalPlan), executionContext);
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
