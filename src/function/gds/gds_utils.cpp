#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "graph/graph.h"
#include "main/settings.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

void GDSUtils::parallelizeFrontierVertexUpdate(processor::ExecutionContext* executionContext,
    std::shared_ptr<FrontierTaskSharedState> sharedState) {
    auto task = std::make_shared<GDSTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        sharedState);
    executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task,
        executionContext);
}

void GDSUtils::runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
    Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& fu, uint64_t maxIters) {
    while (frontiers.getNumNextActiveNodes() > 0 &&
           frontiers.getNextIter() < maxIters) {
        frontiers.beginNewIterationOfUpdates();
        for (auto& [srcNodeTableID, relTableID, dstNodeTableID] :
            graph->getTableCombinations()) {
            frontiers.beginFrontierUpdatesBetweenTables(srcNodeTableID, dstNodeTableID);
            auto sharedState = std::make_shared<FrontierTaskSharedState>(frontiers, graph, fu, relTableID);
            GDSUtils::parallelizeFrontierVertexUpdate(executionContext, sharedState);
        }
        // We put the memory fence here to make sure that updates to pathLengths in this
        // iteration is visible in the next round to all threads.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
}

} // namespace function
} // namespace kuzu
