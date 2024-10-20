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

static void scheduleFrontierTask(table_id_t relTableID, graph::Graph* graph,
    ExtendDirection extendDirection, RJCompState& rjCompState,
    processor::ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto info = FrontierTaskInfo(relTableID, graph, extendDirection, *rjCompState.edgeCompute);
    auto sharedState = std::make_shared<FrontierTaskSharedState>(*rjCompState.frontierPair);
    auto maxThreads =
        clientContext->getCurrentSetting(main::ThreadsSetting::name).getValue<uint64_t>();
    auto task = std::make_shared<FrontierTask>(maxThreads, info, sharedState);
    // GDSUtils::runFrontiersUntilConvergence is called from a GDSCall operator, which is
    // already executed by a worker thread Tm of the task scheduler. So this function is
    // executed by Tm. Because this function will monitor the task and wait for it to
    // complete, running GDS algorithms effectively "loses" Tm. This can even lead to the
    // query processor to halt, e.g., if there is a single worker thread in the system, and
    // more generally decrease the number of worker threads by 1. Therefore, we instruct
    // scheduleTaskAndWaitOrError to start a new thread by passing true as the last
    // argument.
    clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

void GDSUtils::runFrontiersUntilConvergence(processor::ExecutionContext* context,
    RJCompState& rjCompState, graph::Graph* graph, ExtendDirection extendDirection,
    uint64_t maxIters) {
    auto frontierPair = rjCompState.frontierPair.get();
    while (frontierPair->hasActiveNodesForNextLevel() && frontierPair->getNextIter() <= maxIters) {
        frontierPair->beginNewIteration();
        for (auto& relTableIDInfo : graph->getRelTableIDInfos()) {
            switch (extendDirection) {
            case ExtendDirection::FWD: {
                rjCompState.beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
                    relTableIDInfo.toNodeTableID);
                scheduleFrontierTask(relTableIDInfo.relTableID, graph, common::ExtendDirection::FWD,
                    rjCompState, context);
            } break;
            case ExtendDirection::BWD: {
                rjCompState.beginFrontierComputeBetweenTables(relTableIDInfo.toNodeTableID,
                    relTableIDInfo.fromNodeTableID);
                scheduleFrontierTask(relTableIDInfo.relTableID, graph, common::ExtendDirection::BWD,
                    rjCompState, context);
            } break;
            case ExtendDirection::BOTH: {
                rjCompState.beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
                    relTableIDInfo.toNodeTableID);
                scheduleFrontierTask(relTableIDInfo.relTableID, graph, common::ExtendDirection::FWD,
                    rjCompState, context);
                rjCompState.beginFrontierComputeBetweenTables(relTableIDInfo.toNodeTableID,
                    relTableIDInfo.fromNodeTableID);
                scheduleFrontierTask(relTableIDInfo.relTableID, graph, common::ExtendDirection::BWD,
                    rjCompState, context);
            } break;
            default:
                KU_UNREACHABLE;
            }
        }
    }
}

void GDSUtils::runVertexComputeIteration(processor::ExecutionContext* executionContext,
    graph::Graph* graph, VertexCompute& vc) {
    auto clientContext = executionContext->clientContext;
    auto maxThreads =
        clientContext->getCurrentSetting(main::ThreadsSetting::name).getValue<uint64_t>();
    auto info = VertexComputeTaskInfo(vc);
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    for (auto& tableID : graph->getNodeTableIDs()) {
        vc.beginOnTable(tableID);
        sharedState->morselDispatcher.init(tableID, graph->getNumNodes(tableID));
        auto task = std::make_shared<VertexComputeTask>(maxThreads, info, sharedState);
        clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, executionContext,
            true /* launchNewWorkerThread */);
    }
}

} // namespace function
} // namespace kuzu
