#include "function/gds/gds_utils.h"

#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/rec_joins.h"
#include "graph/graph.h"
#include "main/settings.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

void GDSUtils::parallelizeFrontierCompute(processor::ExecutionContext* executionContext,
    std::shared_ptr<FrontierTaskSharedState> sharedState) {
    auto task = std::make_shared<FrontierTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        sharedState);
    // GDSUtils::parallelizeFrontierCompute is called from a GDSCall operator, which is already
    // executed by a worker thread Tm of the task scheduler. So this function is executed by Tm.
    // Because this function will monitor the task and wait for it to complete, running GDS
    // algorithms effectively "loses" Tm. This can even lead to the query processor to halt, e.g.,
    // if there is a single worker thread in the system, and more generally decrease its capacity.
    // Therefore we instruct scheduleTaskAndWaitOrError to start a new thread by passing true as the
    // last argument.
    executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task,
        executionContext, true /* launchNewWorkerThread */);
}

void GDSUtils::runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
    RJCompState& rjCompState, graph::Graph* graph, uint64_t maxIters) {
    auto frontiers = rjCompState.frontiers.get();
    auto fc = rjCompState.frontierCompute.get();
    while (frontiers->hasActiveNodesForNextLevel() && frontiers->getNextIter() <= maxIters) {
        frontiers->beginNewIteration();
        for (auto& relTableIDInfo : graph->getRelTableIDInfos()) {
            rjCompState.beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
                                relTableIDInfo.toNodeTableID);
            auto sharedState = std::make_shared<FrontierTaskSharedState>(*frontiers, graph, *fc,
                relTableIDInfo.relTableID);
            GDSUtils::parallelizeFrontierCompute(executionContext, sharedState);
        }
    }
}

void GDSUtils::runVertexComputeIteration(
    processor::ExecutionContext* executionContext, graph::Graph* graph, VertexCompute& vc) {
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(graph, vc,
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>());
    for (auto& tableID : graph->getNodeTableIDs()) {
        std::cout << "Running runVertexComputeIteration on tableID: " << tableID << std::endl;
        vc.beginComputingOnTable(tableID);
        sharedState->morselizer->init(tableID, graph->getNumNodes(tableID));
        auto task = std::make_shared<VertexComputeTask>(
            executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
                .getValue<uint64_t>(),
            sharedState);
        executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(
            task, executionContext, true /* launchNewWorkerThread */);
    }
}

} // namespace function
} // namespace kuzu
