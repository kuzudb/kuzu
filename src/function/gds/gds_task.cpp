#include "function/gds/gds_task.h"

#include "graph/graph.h"

// TODO(Semih): Remove
#include <iostream>
namespace kuzu {
namespace function {

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    std::unique_ptr<graph::Graph> graph = sharedState->graph->copy();
    auto state = graph->prepareScan(sharedState->relTableIDToScan);
    auto localEc = sharedState->ec.clone();
    while (sharedState->frontiers.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            nodeID_t nodeID = frontierMorsel.getNextNodeID();
            if (sharedState->frontiers.curFrontier->isActive(nodeID)) {
                auto nbrIDs = graph->scanFwd(nodeID, *state);
                for (auto nbrID : nbrIDs) {
                    if (localEc->edgeCompute(nodeID, nbrID)) {
                        sharedState->frontiers.nextFrontier->setActive(nbrID);
                            numApproxActiveNodesForNextIter++;
                    }
                }
            }
        }
    }
    sharedState->frontiers.incrementApproxActiveNodesForNextIter(numApproxActiveNodesForNextIter);
}

VertexComputeTaskSharedState::VertexComputeTaskSharedState(
    graph::Graph* graph, VertexCompute& vc, uint64_t maxThreadsForExecution)
        : graph{graph}, vc{vc} {
    morselizer= std::make_unique<FrontierMorselDispatcher>(maxThreadsForExecution);
}

void VertexComputeTask::run() {
    FrontierMorsel frontierMorsel;
    auto localVc = sharedState->vc.clone();
    while (sharedState->morselizer->getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            nodeID_t nodeID = frontierMorsel.getNextNodeID();
            localVc->vertexCompute(nodeID);
        }
    }
    localVc->finalizeWorkerThread();
    std::cout << "finalizeWorkerThread() called" << std::endl;
}
} // namespace function
} // namespace kuzu
