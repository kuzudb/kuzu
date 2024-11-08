#include "function/gds/gds_task.h"

#include "common/assert.h"
#include "common/enums/extend_direction.h"
#include "graph/graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static uint64_t computeScanResult(nodeID_t sourceNodeID, graph::GraphScanState::Chunk& chunk,
    EdgeCompute& ec, FrontierPair& frontierPair, bool isFwd) {
    auto activeNodes = ec.edgeCompute(sourceNodeID, chunk, isFwd);
    frontierPair.getNextFrontierUnsafe().setActive(activeNodes);
    return chunk.size();
}

FrontierTask::FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo& info,
    std::shared_ptr<FrontierTaskSharedState> sharedState)
    : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {}

void FrontierTask::run() {
    FrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    auto graph = info.graph;
    std::unique_ptr<graph::GraphScanState> scanState;
    if (info.direction == ExtendDirection::FWD) {
        scanState = info.graph->prepareScanFwd(info.relTableIDToScan);
    } else if (info.direction == ExtendDirection::BWD) {
        scanState = info.graph->prepareScanBwd(info.relTableIDToScan);
    } else {
        KU_UNREACHABLE;
    }
    auto localEc = info.edgeCompute.copy();
    while (sharedState->frontierPair.getNextRangeMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextOffset()) {
            common::nodeID_t nodeID = frontierMorsel.getNextNodeID();
            if (sharedState->frontierPair.curFrontier->isActive(nodeID.offset)) {
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
}
} // namespace function
} // namespace kuzu
