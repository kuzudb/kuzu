#include "function/gds/gds_task.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "graph/graph.h"
#include "function/gds/frontier_morsel.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void FrontierTask::run() {
    FrontierMorsel morsel;
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareRelScan(info.relEntry, info.nbrEntry, info.propertyToScan);
    auto ec = info.edgeCompute.copy();
    switch (info.direction) {
    case ExtendDirection::FWD: {
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            for (auto offset = morsel.getBeginOffset(); offset < morsel.getEndOffset(); ++offset) {
                if (!sharedState->frontierPair.isActive(offset)) {
                    continue;
                }
                nodeID_t nodeID = {offset, info.boundEntry->getTableID()};
                for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                    auto activeNodes = ec->edgeCompute(nodeID, chunk, true);
                    sharedState->frontierPair.addNodesToNextDenseFrontier(activeNodes);
                    numActiveNodes += activeNodes.size();
                }
            }
        }
    } break;
    case ExtendDirection::BWD: {
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            for (auto offset = morsel.getBeginOffset(); offset < morsel.getEndOffset(); ++offset) {
                if (!sharedState->frontierPair.isActive(offset)) {
                    continue;
                }
                nodeID_t nodeID = {offset, info.boundEntry->getTableID()};
                for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                    auto activeNodes = ec->edgeCompute(nodeID, chunk, false);
                    sharedState->frontierPair.addNodesToNextDenseFrontier(activeNodes);
                    numActiveNodes += activeNodes.size();
                }
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
    }
}

void FrontierTask::runSparse() {
    auto numActiveNodes = 0u;
    auto graph = info.graph;
    auto scanState = graph->prepareRelScan(info.relEntry, info.nbrEntry, info.propertyToScan);
    auto ec = info.edgeCompute.copy();
    switch (info.direction) {
    case ExtendDirection::FWD: {
        for (const auto offset : sharedState->frontierPair.getActiveNodes()) {
            auto nodeID = nodeID_t{offset, info.boundEntry->getTableID()};
            for (auto chunk : graph->scanFwd(nodeID, *scanState)) {
                auto activeNodes = ec->edgeComputeSparse(nodeID, chunk, true);
                sharedState->frontierPair.addNodesToNextSpareFrontier(activeNodes);
                numActiveNodes += activeNodes.size();
            }
        }
    } break;
    case ExtendDirection::BWD: {
        for (auto& offset : sharedState->frontierPair.getActiveNodes()) {
            auto nodeID = nodeID_t{offset, info.boundEntry->getTableID()};
            for (auto chunk : graph->scanBwd(nodeID, *scanState)) {
                auto activeNodes = ec->edgeComputeSparse(nodeID, chunk, false);
                sharedState->frontierPair.addNodesToNextSpareFrontier(activeNodes);
                numActiveNodes += activeNodes.size();
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (numActiveNodes) {
        sharedState->frontierPair.setActiveNodesForNextIter();
    }
}

void VertexComputeTask::run() {
    FrontierMorsel morsel;
    auto graph = info.graph;
    auto localVc = info.vc.copy();
    if (info.hasPropertiesToScan()) {
        auto scanState = graph->prepareVertexScan(info.tableEntry, info.propertiesToScan);
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            for (auto chunk :
                graph->scanVertices(morsel.getBeginOffset(), morsel.getEndOffset(), *scanState)) {
                localVc->vertexCompute(chunk);
            }
        }
    } else {
        while (sharedState->morselDispatcher.getNextRangeMorsel(morsel)) {
            localVc->vertexCompute(morsel.getBeginOffset(), morsel.getEndOffset(),
                info.tableEntry->getTableID());
        }
    }
}

} // namespace function
} // namespace kuzu
