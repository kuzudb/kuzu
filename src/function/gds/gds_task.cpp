#include "function/gds/gds_task.h"

#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t runEdgeCompute(nodeID_t sourceNodeID, graph::NbrScanState::Chunk& nbrChunk,
    EdgeCompute& ec, FrontierPair& frontierPair, bool isFwd, SparseFrontier& localFrontier) {
    auto activeNodes = ec.edgeCompute(sourceNodeID, nbrChunk, isFwd);
    frontierPair.addNodesToNextDenseFrontier(activeNodes);
    localFrontier.addNodes(activeNodes);
    localFrontier.checkSampleSize();
    return nbrChunk.size();
}

void FrontierTask::run() {
    FrontierMorsel morsel;
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableID, info.edgePropertyIdx);
    auto localEc = info.edgeCompute.copy();
    SparseFrontier localFrontier;
    localFrontier.pinTableID(info.nbrTableID);
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
                    numActiveNodes += runEdgeCompute(nodeID, chunk, *localEc,
                        sharedState->frontierPair, true, localFrontier);
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
                    numActiveNodes += runEdgeCompute(nodeID, chunk, *localEc,
                        sharedState->frontierPair, false, localFrontier);
                }
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
        sharedState->frontierPair.mergeLocalFrontier(localFrontier);
    }
}

void FrontierTask::runSparse() {
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareScan(info.relTableID, info.edgePropertyIdx);
    auto localEc = info.edgeCompute.copy();
    SparseFrontier localFrontier;
    localFrontier.pinTableID(info.nbrTableID);
    auto& curFrontier = sharedState->frontierPair.getCurSparseFrontier();
    switch (info.direction) {
    case ExtendDirection::FWD: {
        for (auto& offset : curFrontier.getOffsetSet()) {
            auto nodeID = nodeID_t{offset, curFrontier.getTableID()};
            for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                numActiveNodes += runEdgeCompute(nodeID, chunk, *localEc, sharedState->frontierPair,
                    true, localFrontier);
            }
        }
    } break;
    case ExtendDirection::BWD: {
        for (auto& offset : curFrontier.getOffsetSet()) {
            auto nodeID = nodeID_t{offset, curFrontier.getTableID()};
            for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                numActiveNodes += runEdgeCompute(nodeID, chunk, *localEc, sharedState->frontierPair,
                    false, localFrontier);
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
        sharedState->frontierPair.mergeLocalFrontier(localFrontier);
    }
}

void VertexComputeTask::run() {
    FrontierMorsel morsel;
    auto graph = sharedState->graph;
    auto localVc = info.vc.copy();
    auto tableID = sharedState->morselDispatcher.getTableID();
    if (info.hasPropertiesToScan()) {
        auto scanState = graph->prepareVertexScan(tableID, info.propertiesToScan);
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            for (auto chunk :
                graph->scanVertices(morsel.getBeginOffset(), morsel.getEndOffset(), *scanState)) {
                localVc->vertexCompute(chunk);
            }
        }
    } else {
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            localVc->vertexCompute(morsel.getBeginOffset(), morsel.getEndOffset(), tableID);
        }
    }
}

} // namespace function
} // namespace kuzu
