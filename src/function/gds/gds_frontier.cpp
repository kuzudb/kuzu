#include "function/gds/gds_frontier.h"

#include "function/gds/gds_utils.h"
#include "graph/on_disk_graph.h"
#include "processor/execution_context.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

void SparseFrontier::pinTableID(table_id_t tableID) {
    curData = sparseObjects.getData(tableID);
}

void SparseFrontier::addNode(nodeID_t nodeID, iteration_t iter) {
    KU_ASSERT(curData);
    curData->insert({nodeID.offset, iter});
}

void SparseFrontier::addNodes(const std::vector<nodeID_t>& nodeIDs, iteration_t iter) {
    KU_ASSERT(curData);
    for (auto nodeID : nodeIDs) {
        curData->insert({nodeID.offset, iter});
    }
}

iteration_t SparseFrontier::getIteration(offset_t offset) const {
    KU_ASSERT(curData);
    if (!curData->contains(offset)) {
        return FRONTIER_UNVISITED;
    }
    return curData->at(offset);
}

void DenseFrontier::init(ExecutionContext* context, Graph* graph, iteration_t val) {
    for (const auto& [tableID, maxOffset] : nodeMaxOffsetMap) {
        denseObjects.allocate(tableID, maxOffset, context->clientContext->getMemoryManager());
    }
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*this, val);
    GDSUtils::runVertexCompute(context, graph, *vc);
}

void DenseFrontier::pinTableID(table_id_t tableID) {
    curData = denseObjects.getData(tableID);
}

void DenseFrontier::addNode(nodeID_t nodeID, iteration_t iter) {
    KU_ASSERT(curData);
    curData[nodeID.offset].store(iter, std::memory_order_relaxed);
}

void DenseFrontier::addNodes(const std::vector<nodeID_t>& nodeIDs, iteration_t iter) {
    KU_ASSERT(curData);
    for (auto nodeID : nodeIDs) {
        curData[nodeID.offset].store(iter, std::memory_order_relaxed);
    }
}

iteration_t DenseFrontier::getIteration(offset_t offset) const {
    KU_ASSERT(curData);
    return curData[offset].load(std::memory_order_relaxed);
}

std::shared_ptr<DenseFrontier> DenseFrontier::getUninitializedFrontier(
    ExecutionContext* context, Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    return std::make_shared<DenseFrontier>(graph->getMaxOffsetMap(transaction));
}

std::shared_ptr<DenseFrontier> DenseFrontier::getUnvisitedFrontier(ExecutionContext* context,
    Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    auto frontier = std::make_shared<DenseFrontier>(graph->getMaxOffsetMap(transaction));
    frontier->init(context, graph, FRONTIER_UNVISITED);
    return frontier;
}

std::shared_ptr<DenseFrontier> DenseFrontier::getVisitedFrontier(ExecutionContext* context,
    Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    auto frontier = std::make_shared<DenseFrontier>(graph->getMaxOffsetMap(transaction));
    frontier->init(context, graph, FRONTIER_INITIAL_VISITED);
    return frontier;
}

std::shared_ptr<DenseFrontier> DenseFrontier::getVisitedFrontier(ExecutionContext* context,
    Graph* graph, NodeOffsetMaskMap* maskMap) {
    if (maskMap == nullptr) {
        return getVisitedFrontier(context, graph);
    }
    auto tx = context->clientContext->getTransaction();
    auto frontier = std::make_shared<DenseFrontier>(graph->getMaxOffsetMap(tx));
    frontier->init(context, graph, FRONTIER_UNVISITED);
    // TODO(Xiyang): we should use a vertex compute to do the following.
    for (auto [tableID, numNodes] : graph->getMaxOffsetMap(tx)) {
        frontier->pinTableID(tableID);
        if (!maskMap->containsTableID(tableID)) {
            for (auto i = 0u; i < numNodes; ++i) {
                frontier->curData[i].store(FRONTIER_INITIAL_VISITED);
            }
        } else {
            auto mask = maskMap->getOffsetMask(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                if (mask->isMasked(i)) {
                    frontier->curData[i].store(FRONTIER_INITIAL_VISITED);
                }
            }
        }
    }
    return frontier;
}

void DenseFrontierReference::pinTableID(table_id_t tableID) {
    curData = denseObjects.getData(tableID);
}

void DenseFrontierReference::addNode(nodeID_t nodeID, iteration_t iter) {
    KU_ASSERT(curData);
    curData[nodeID.offset].store(iter, std::memory_order_relaxed);
}

void DenseFrontierReference::addNodes(const std::vector<nodeID_t>& nodeIDs,
    iteration_t iter) {
    KU_ASSERT(curData);
    for (auto nodeID : nodeIDs) {
        curData[nodeID.offset].store(iter, std::memory_order_relaxed);
    }
}

iteration_t DenseFrontierReference::getIteration(offset_t offset) const {
    KU_ASSERT(curData);
    return curData[offset].load(std::memory_order_relaxed);
}

bool PathLengthsInitVertexCompute::beginOnTable(table_id_t tableID) {
    frontier.pinTableID(tableID);
    return true;
}

void PathLengthsInitVertexCompute::vertexCompute(offset_t startOffset,
    offset_t endOffset, table_id_t) {
    for (auto i = startOffset; i < endOffset; ++i) {
        frontier.curData[i].store(FRONTIER_INITIAL_VISITED);
    }
}

void FrontierPair::beginNewIteration() {
    std::unique_lock<std::mutex> lck{mtx};
    curIter++;
    hasActiveNodesForNextIter_.store(false);
    beginNewIterationInternalNoLock();
}

void FrontierPair::beginFrontierComputeBetweenTables(table_id_t curTableID,
    table_id_t nextTableID) {
    pinCurrentFrontier(curTableID);
    pinNextFrontier(nextTableID);
}

void FrontierPair::pinCurrentFrontier(table_id_t tableID) {
    currentFrontier->pinTableID(tableID);
}

void FrontierPair::pinNextFrontier(table_id_t tableID) {
    nextFrontier->pinTableID(tableID);
}

void FrontierPair::addNodeToNextFrontier(nodeID_t nodeID) {
    nextFrontier->addNode(nodeID, curIter);
}

void FrontierPair::addNodesToNextFrontier(const std::vector<nodeID_t>& nodeIDs) {
    nextFrontier->addNodes(nodeIDs, curIter);
}

iteration_t FrontierPair::getNextFrontierValue(offset_t offset) {
    return nextFrontier->getIteration(offset);
}

bool FrontierPair::isActiveOnCurrentFrontier(offset_t offset) {
    return currentFrontier->getIteration(offset) == curIter - 1;
}

void SPFrontierPair::beginNewIterationInternalNoLock() {
    switch (state) {
    case GDSDensityState::SPARSE: {
        std::swap(curSparseFrontier, nextSparseFrontier);
        currentFrontier = curSparseFrontier.get();
        nextFrontier = nextSparseFrontier.get();
    } break;
    case GDSDensityState::DENSE: {
        std::swap(curDenseFrontier, nextDenseFrontier);
        currentFrontier = curDenseFrontier.get();
        nextFrontier = nextDenseFrontier.get();
    } break;
    default:
        KU_UNREACHABLE;
    }
}

offset_t SPFrontierPair::getNumActiveNodesInCurrentFrontier(NodeOffsetMaskMap& mask) {
    auto result = 0u;
    for (auto& [tableID, maxNumNodes] : denseFrontier->nodeMaxOffsetMap) {
        currentFrontier->pinTableID(tableID);
        if (!mask.containsTableID(tableID)) {
            continue;
        }
        auto offsetMask = mask.getOffsetMask(tableID);
        for (auto offset = 0u; offset < maxNumNodes; ++offset) {
            if (isActiveOnCurrentFrontier(offset)) {
                result += offsetMask->isMasked(offset);
            }
        }
    }
    return result;
}

void SPFrontierPair::switchToDense(ExecutionContext* context, graph::Graph* graph) {
    KU_ASSERT(state == GDSDensityState::SPARSE);
    state = GDSDensityState::DENSE;
    for (auto& [tableID, map] : nextSparseFrontier->sparseObjects) {
        nextDenseFrontier->pinTableID(tableID);
        for (auto [offset, iter] : map) {
            nextDenseFrontier->curData[offset].store(iter);
        }
    }
}

void VarLengthFrontierPair::beginNewIterationInternalNoLock() {
    switch (state) {
    case GDSDensityState::SPARSE: {
        std::swap(curSparseFrontier, nextSparseFrontier);
        currentFrontier = curSparseFrontier.get();
        nextFrontier = nextSparseFrontier.get();
    } break;
    case GDSDensityState::DENSE: {
        std::swap(curDenseFrontier, nextDenseFrontier);
        currentFrontier = curDenseFrontier.get();
        nextFrontier = nextDenseFrontier.get();
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void VarLengthFrontierPair::switchToDense(ExecutionContext* context, Graph* graph) {
    KU_ASSERT(state == GDSDensityState::SPARSE);
    state = GDSDensityState::DENSE;
    for (auto& [tableID, map] : nextSparseFrontier->sparseObjects) {
        nextDenseFrontier->pinTableID(tableID);
        for (auto [offset, iter] : map) {
            nextDenseFrontier->curData[offset].store(iter);
        }
    }
}


void DenseFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curDenseFrontier, nextDenseFrontier);
}

static constexpr uint64_t EARLY_TERM_NUM_NODES_THRESHOLD = 100;

bool SPEdgeCompute::terminate(NodeOffsetMaskMap& maskMap) {
    auto targetNumNodes = maskMap.getNumMaskedNode();
    if (targetNumNodes > EARLY_TERM_NUM_NODES_THRESHOLD) {
        // Skip checking if it's unlikely to early terminate.
        return false;
    }
    numNodesReached += frontierPair->getNumActiveNodesInCurrentFrontier(maskMap);
    return numNodesReached == targetNumNodes;
}

} // namespace function
} // namespace kuzu
