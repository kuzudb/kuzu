#include "function/gds/gds_task.h"

#include "common/data_chunk/sel_vector.h"
#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(nodeID_t sourceNodeID, std::span<const nodeID_t> nbrNodeIDs,
    std::span<const relID_t> edgeIDs, SelectionVector& mask, EdgeCompute& ec,
    FrontierPair& frontierPair, bool isFwd) {
    KU_ASSERT(nbrNodeIDs.size() == edgeIDs.size());
    ec.edgeCompute(sourceNodeID, nbrNodeIDs, edgeIDs, mask, isFwd);
    frontierPair.getNextFrontierUnsafe().setActive(mask, nbrNodeIDs);
    return mask.getSelSize();
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
                    for (auto [nodes, edges, mask] : graph->scanFwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            mask, *localEc, sharedState->frontierPair, true);
                    }
                } break;
                case ExtendDirection::BWD: {
                    for (auto [nodes, edges, mask] : graph->scanBwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, nodes, edges,
                            mask, *localEc, sharedState->frontierPair, false);
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

void VertexComputeTask::run() {
    FrontierMorsel frontierMorsel;
    auto localVc = info.vc.copy();
    while (sharedState->morselDispatcher.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            localVc->vertexCompute(nodeID);
        }
    }
    localVc->finalizeWorkerThread();
}
} // namespace function
} // namespace kuzu
