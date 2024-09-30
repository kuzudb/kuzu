#include "function/gds/gds_task.h"

#include "graph/graph.h"

namespace kuzu {
namespace function {

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    std::unique_ptr<graph::Graph> graph = sharedState->graph->copy();
    auto state = graph->prepareScan(sharedState->relTableIDToScan);
    auto scanResult = graph::GraphScanResult();
    auto localEc = sharedState->ec.copy();
    while (sharedState->frontierPair.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            if (sharedState->frontierPair.curFrontier->isActive(nodeID)) {
                graph->scanFwd(nodeID, *state, scanResult);
                for (auto i = 0u; i < scanResult.size(); ++i) {
                    auto nbrNodeID = scanResult.nbrNodeIDs[i];
                    auto edgeID = scanResult.edgeIDs[i];
                    if (localEc->edgeCompute(nodeID, nbrNodeID, edgeID)) {
                        sharedState->frontierPair.nextFrontier->setActive(nbrNodeID);
                        numApproxActiveNodesForNextIter++;
                    }
                }
            }
        }
    }
    sharedState->frontierPair.incrementApproxActiveNodesForNextIter(
        numApproxActiveNodesForNextIter);
}

VertexComputeTaskSharedState::VertexComputeTaskSharedState(graph::Graph* graph, VertexCompute& vc,
    uint64_t maxThreadsForExecution)
    : graph{graph}, vc{vc} {
    morselDispatcher = std::make_unique<FrontierMorselDispatcher>(maxThreadsForExecution);
}

void VertexComputeTask::run() {
    FrontierMorsel frontierMorsel;
    auto localVc = sharedState->vc.copy();
    while (sharedState->morselDispatcher->getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            localVc->vertexCompute(nodeID);
        }
    }
    localVc->finalizeWorkerThread();
}
} // namespace function
} // namespace kuzu
