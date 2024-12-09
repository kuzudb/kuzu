#include "function/gds/gds_task.h"

#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(nodeID_t sourceNodeID, graph::NbrScanState::Chunk& nbrChunk,
    EdgeCompute& ec, FrontierPair& frontierPair, bool isFwd) {
    auto activeNodes = ec.edgeCompute(sourceNodeID, nbrChunk, isFwd);
    frontierPair.getNextFrontierUnsafe().setActive(activeNodes);
    return nbrChunk.size();
}

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableIDToScan, info.edgePropertyIdx);
    auto localEc = info.edgeCompute.copy();
    while (sharedState->frontierPair.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            if (sharedState->frontierPair.curFrontier->isActive(nodeID.offset)) {
                switch (info.direction) {
                case ExtendDirection::FWD: {
                    for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                        numActiveNodes += computeScanResult(nodeID, chunk, *localEc,
                            sharedState->frontierPair, true);
                    }
                } break;
                case ExtendDirection::BWD: {
                    for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                        numActiveNodes += computeScanResult(nodeID, chunk, *localEc,
                            sharedState->frontierPair, false);
                    }
                } break;
                default:
                    KU_UNREACHABLE;
                }
            }
        }
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
    }
}

void VertexComputeTask::run() {
    FrontierMorsel frontierMorsel;
    auto graph = sharedState->graph;
    auto localVc = info.vc.copy();
    auto tableID = sharedState->morselDispatcher.getTableID();
    if (info.hasPropertiesToScan()) {
        auto scanState = graph->prepareVertexScan(tableID, info.propertiesToScan);
        while (sharedState->morselDispatcher.getNextRangeMorsel(frontierMorsel)) {
            for (auto chunk : graph->scanVertices(frontierMorsel.getBeginOffset(),
                     frontierMorsel.getEndOffsetExclusive(), *scanState)) {
                localVc->vertexCompute(chunk);
            }
        }
    } else {
        while (sharedState->morselDispatcher.getNextRangeMorsel(frontierMorsel)) {
            localVc->vertexCompute(frontierMorsel.getBeginOffset(),
                frontierMorsel.getEndOffsetExclusive(), tableID);
        }
    }
}

} // namespace function
} // namespace kuzu
