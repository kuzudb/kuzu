#include "function/gds/gds_task.h"

#include "common/data_chunk/sel_vector.h"
#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(nodeID_t sourceNodeID, graph::GraphScanState::Chunk& chunk,
    EdgeCompute& ec, FrontierPair& frontierPair, bool isFwd) {
    KU_ASSERT(chunk.nbrNodes.size() == chunk.edges.size());
    ec.edgeCompute(sourceNodeID, chunk.nbrNodes, chunk.edges, chunk.selVector, isFwd,
        chunk.propertyVector);
    frontierPair.getNextFrontierUnsafe().setActive(chunk.selVector, chunk.nbrNodes);
    return chunk.selVector.getSelSize();
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
                    for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, chunk,
                            *localEc, sharedState->frontierPair, true);
                    }
                } break;
                case ExtendDirection::BWD: {
                    for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, chunk,
                            *localEc, sharedState->frontierPair, false);
                    }
                } break;
                case ExtendDirection::BOTH: {
                    for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, chunk,
                            *localEc, sharedState->frontierPair, true);
                    }
                    for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                        numApproxActiveNodesForNextIter += computeScanResult(nodeID, chunk,
                            *localEc, sharedState->frontierPair, false);
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
    auto graph = sharedState->graph;
    std::vector<std::string> propertiesToScan;
    auto scanState =
        graph->prepareVertexScan(sharedState->morselDispatcher->getTableID(), propertiesToScan);
    auto localVc = sharedState->vc.copy();
    while (sharedState->morselDispatcher->getNextRangeMorsel(frontierMorsel)) {
        for (auto [nodeIDs, propertyVectors] : graph->scanVertices(frontierMorsel.getBeginOffset(),
                 frontierMorsel.getEndOffsetExclusive(), *scanState)) {
            localVc->vertexCompute(nodeIDs, propertyVectors);
        }
    }
    localVc->finalizeWorkerThread();
}
} // namespace function
} // namespace kuzu
