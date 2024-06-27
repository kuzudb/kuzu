#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "function/gds/new_frontier.h"
#include "graph/graph.h"
#include "main/settings.h"

// TODO(Semih): Remove
#include <iostream>

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

void GDSUtils::parallelizeFrontierVertexUpdate(processor::ExecutionContext* executionContext,
    std::shared_ptr<FrontierTaskSharedState> sharedState) {
//    Frontiers& frontiers,  graph::Graph* graph, FrontierUpdateFn& vu) {
    std::cout << " GDSUtils::parallelizeFrontierVertexUpdate called. " << std::endl;
//    auto sharedState = std::make_shared<FrontierTaskSharedState>(frontiers, graph, vu);
    auto task = std::make_shared<FrontierTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        sharedState);
    executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task,
        executionContext);
}

void GDSUtils::runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
    Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu, uint64_t maxIters) {
    std::cout << "GDSUtils::runFrontiersUntilConvergence() called. maxIters: " << maxIters
              << " frontiers.getNumNextActiveNodes(): " << frontiers.getNumNextActiveNodes() << std::endl;
    while (frontiers.getNumNextActiveNodes() > 0 &&
           frontiers.getNextIter() < maxIters) {
        frontiers.beginNewIterationOfUpdates();
        for (auto& [srcNodeTableID, relTableID, dstNodeTableID] :
            graph->getTableCombinations()) {
            std::cout << "starting frontier updates between tables: " << srcNodeTableID << " and "
                      << dstNodeTableID << std::endl;
            frontiers.beginFrontierUpdatesBetweenTables(srcNodeTableID, dstNodeTableID);
            auto sharedState = std::make_shared<FrontierTaskSharedState>(frontiers, graph, vu, relTableID);
            GDSUtils::parallelizeFrontierVertexUpdate(executionContext, sharedState);
        }
        // We put the memory fence here to make sure that updates to pathLengths in this
        // iteration is visible in the next round to all threads.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        std::cout << "numActiveNodes: " << frontiers.getNumNextActiveNodes() << std::endl;
    }
}

} // namespace function
} // namespace kuzu