#include "function/gds/gds_task.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(nodeID_t sourceNodeID,
    const std::span<const nodeID_t>& nbrNodeIDs, const std::span<const relID_t>& edgeIDs,
    EdgeCompute& ec, FrontierPair& frontierPair) {
    KU_ASSERT(nbrNodeIDs.size() == edgeIDs.size());
    uint64_t numComputedResult = 0;
    for (size_t i = 0; i < nbrNodeIDs.size(); i++) {
        const auto& nbrNodeID = nbrNodeIDs[i];
        const auto& edgeID = edgeIDs[i];
        if (ec.edgeCompute(sourceNodeID, nbrNodeID, edgeID)) {
            frontierPair.getNextFrontierUnsafe().setActive(nbrNodeID);
            numComputedResult++;
        }
    }
    return numComputedResult;
}

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableIDToScan);
    auto localEc = info.edgeCompute.copy();
    while (sharedState->frontierPair.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            if (sharedState->frontierPair.curFrontier->isActive(nodeID)) {
                switch (info.direction) {
                case ExtendDirection::FWD: {
                    for (const auto [nodes, edges] : graph->scanFwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            *localEc, sharedState->frontierPair);
                    }
                } break;
                case ExtendDirection::BWD: {
                    for (const auto [nodes, edges] : graph->scanBwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            *localEc, sharedState->frontierPair);
                    }
                } break;
                case ExtendDirection::BOTH: {
                    for (const auto [nodes, edges] : graph->scanFwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            *localEc, sharedState->frontierPair);
                    }
                    for (const auto [nodes, edges] : graph->scanBwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            *localEc, sharedState->frontierPair);
                    }
                } break;
                default:
                    KU_UNREACHABLE;
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
