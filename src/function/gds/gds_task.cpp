#include "function/gds/gds_task.h"

#include "graph/graph.h"
#include "common/types/internal_id_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t runEdgeCompute(nodeID_t sourceNodeID, graph::NbrScanState::Chunk& nbrChunk,
    EdgeCompute& ec, FrontierPair& frontierPair, bool isFwd, LocalFrontierSample& localFrontierSample) {
    auto activeNodes = ec.edgeCompute(sourceNodeID, nbrChunk, isFwd);
    frontierPair.addNodesToNextDenseFrontier(activeNodes);
    localFrontierSample.addNodes(activeNodes);
    localFrontierSample.checkSampleSize();
    return nbrChunk.size();
}

// TODO: rename to runDense
void FrontierTask::run() {
    FrontierMorsel morsel;
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableIDToScan, info.edgePropertyIdx);
    auto localEc = info.edgeCompute.copy();
    LocalFrontierSample localFrontierSample;
    auto& curFrontier = sharedState->frontierPair.getCurDenseFrontier();
    switch (info.direction) {
    case ExtendDirection::FWD: {
        while (sharedState->frontierPair.getNextRangeMorsel(morsel)) {
            for (auto offset = morsel.getBeginOffset(); offset < morsel.getEndOffset(); ++offset) {
                if (!curFrontier.isActive(offset)) {
                    continue;
                }
                nodeID_t nodeID = {offset, morsel.getTableID()};
                for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                    numActiveNodes += runEdgeCompute(nodeID, chunk,
                        *localEc, sharedState->frontierPair, true, localFrontierSample);
                }
            }
        }
    } break;
    case ExtendDirection::BWD: {
        while (sharedState->frontierPair.getNextRangeMorsel(morsel)) {
            for (auto offset = morsel.getBeginOffset(); offset < morsel.getEndOffset(); ++offset) {
                if (!curFrontier.isActive(offset)) {
                    continue;
                }
                nodeID_t nodeID = {offset, morsel.getTableID()};
                for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                    numActiveNodes += runEdgeCompute(nodeID, chunk,
                        *localEc, sharedState->frontierPair, false, localFrontierSample);
                }
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
        sharedState->frontierPair.mergeLocalFrontier(localFrontierSample);
    }
}

void FrontierTask::runSparse() {
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableIDToScan);
    auto localEc = info.edgeCompute.copy();
    LocalFrontierSample localFrontierSample;
    auto& curFrontier = sharedState->frontierPair.getCurSparseFrontier();
    switch (info.direction) {
    case ExtendDirection::FWD: {
        for (auto& offset : curFrontier.getOffsetSet()) {
            auto nodeID = nodeID_t{offset, curFrontier.getTableID()};
            for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                numActiveNodes += runEdgeCompute(nodeID, chunk,
                    *localEc, sharedState->frontierPair, true, localFrontierSample);
            }
        }
    } break;
    case ExtendDirection::BWD: {
        for (auto& offset : curFrontier.getOffsetSet()) {
            auto nodeID = nodeID_t{offset, curFrontier.getTableID()};
            for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                numActiveNodes += runEdgeCompute(nodeID, chunk,
                    *localEc, sharedState->frontierPair, false, localFrontierSample);
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
        sharedState->frontierPair.mergeLocalFrontier(localFrontierSample);
    }
}

void VertexComputeTask::run() {
    FrontierMorsel morsel;
    auto graph = sharedState->graph;
    auto scanState =
        graph->prepareVertexScan(sharedState->morselDispatcher.getTableID(), info.propertiesToScan);
    auto localVc = info.vc.copy();
    while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
        for (auto chunk : graph->scanVertices(morsel.getBeginOffset(),
                 morsel.getEndOffset(), *scanState)) {
            localVc->vertexCompute(chunk);
        }
    }
}
} // namespace function
} // namespace kuzu
