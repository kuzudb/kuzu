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

FrontierMorselDispatcher::FrontierMorselDispatcher(uint64_t maxThreads)
    : maxOffset{INVALID_OFFSET}, maxThreads{maxThreads}, morselSize(UINT64_MAX) {
    nextOffset.store(INVALID_OFFSET);
}

void FrontierMorselDispatcher::init(offset_t _maxOffset) {
    maxOffset = _maxOffset;
    nextOffset.store(0u);
    // Frontier size calculation: The ideal scenario is to have k^2 many morsels where k
    // the number of maximum threads that could be working on this frontier. However, if
    // that is too small then we default to MIN_FRONTIER_MORSEL_SIZE.
    auto idealMorselSize =
        maxOffset / std::max(MIN_NUMBER_OF_FRONTIER_MORSELS, maxThreads * maxThreads);
    morselSize = std::max(MIN_FRONTIER_MORSEL_SIZE, idealMorselSize);
}

bool FrontierMorselDispatcher::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    auto beginOffset = nextOffset.fetch_add(morselSize, std::memory_order_acq_rel);
    if (beginOffset >= maxOffset) {
        return false;
    }
    auto endOffset = beginOffset + morselSize > maxOffset ? maxOffset : beginOffset + morselSize;
    frontierMorsel.init(beginOffset, endOffset);
    return true;
}

void SparseFrontier::pinTableID(table_id_t tableID) {
    if (!tableIDToOffsetMap.contains(tableID)) {
        tableIDToOffsetMap.insert({tableID, std::unordered_set<offset_t>{}});
    }
    curOffsetSet = &tableIDToOffsetMap.at(tableID);
}

PathLengths::PathLengths(const table_id_map_t<offset_t>& nodeMaxOffsetMap,
    storage::MemoryManager* mm)
    : nodeMaxOffsetMap{nodeMaxOffsetMap} {
    for (const auto& [tableID, maxOffset] : nodeMaxOffsetMap) {
        auto memBuffer = mm->allocateBuffer(false, maxOffset * sizeof(std::atomic<iteration_t>));
        buffer.insert({tableID, std::move(memBuffer)});
    }
}

PathLengths::~PathLengths() = default;

std::shared_ptr<PathLengths> PathLengths::getUninitializedFrontier(
    ExecutionContext* context, Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    return std::make_shared<PathLengths>(graph->getMaxOffsetMap(transaction), mm);
}

std::shared_ptr<PathLengths> PathLengths::getUnvisitedFrontier(ExecutionContext* context,
    Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto frontier = std::make_shared<PathLengths>(graph->getMaxOffsetMap(transaction), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*frontier, UNVISITED);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return frontier;
}

std::shared_ptr<PathLengths> PathLengths::getVisitedFrontier(ExecutionContext* context,
    Graph* graph) {
    auto transaction = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto frontier = std::make_shared<PathLengths>(graph->getMaxOffsetMap(transaction), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*frontier, INITIAL_VISITED);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return frontier;
}

std::shared_ptr<PathLengths> PathLengths::getVisitedFrontier(ExecutionContext* context,
    Graph* graph, NodeOffsetMaskMap* maskMap) {
    if (maskMap == nullptr) {
        return getVisitedFrontier(context, graph);
    }
    auto tx = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto frontier = std::make_shared<PathLengths>(graph->getMaxOffsetMap(tx), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*frontier, UNVISITED);
    GDSUtils::runVertexCompute(context, graph, *vc);
    // TODO(Xiyang): we should use a vertex compute to do the following.
    for (auto [tableID, numNodes] : graph->getMaxOffsetMap(tx)) {
        frontier->pinTableID(tableID);
        if (!maskMap->containsTableID(tableID)) {
            for (auto i = 0u; i < numNodes; ++i) {
                frontier->setIteration(i, INITIAL_VISITED);
            }
        } else {
            auto mask = maskMap->getOffsetMask(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                if (mask->isMasked(i)) {
                    frontier->setIteration(i, INITIAL_VISITED);
                }
            }
        }
    }
    return frontier;
}

std::atomic<iteration_t>* PathLengths::getMaskData(table_id_t tableID) {
    KU_ASSERT(buffer.contains(tableID));
    return reinterpret_cast<std::atomic<iteration_t>*>(buffer.at(tableID)->getData());
}

bool PathLengthsInitVertexCompute::beginOnTable(table_id_t tableID) {
    pathLengths.pinTableID(tableID);
    return true;
}

void PathLengthsInitVertexCompute::vertexCompute(offset_t startOffset,
    offset_t endOffset, table_id_t) {
    for (auto i = startOffset; i < endOffset; ++i) {
        pathLengths.setIteration(i, val);
    }
}

void FrontierPair::beginNewIteration() {
    std::unique_lock<std::mutex> lck{mtx};
    curIter.fetch_add(1u);
    hasActiveNodesForNextIter_.store(false);
    beginNewIterationInternalNoLock();
}

void FrontierPair::initState() {
    hasActiveNodesForNextIter_.store(false);
    curIter.store(0u);
}

void SPFrontierPair::pinCurrentFrontier(table_id_t tableID) {
    curSparseFrontier->pinTableID(tableID);
    curDenseFrontier = pathLengths->getMaskData(tableID);
}

void SPFrontierPair::pinNextFrontier(table_id_t tableID) {
    nextSparseFrontier->pinTableID(tableID);
    nextDenseFrontier = pathLengths->getMaskData(tableID);
}

void SPFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curSparseFrontier, nextSparseFrontier);
}

void SPFrontierPair::addNodeToNextSparseFrontier(nodeID_t nodeID) {
    nextSparseFrontier->addNode(nodeID);
}

void SPFrontierPair::addNodesToNextSpareFrontier(const std::vector<nodeID_t>& nodeIDs) {
    nextSparseFrontier->addNodes(nodeIDs);
}

void SPFrontierPair::addNodesToNextDenseFrontier(
    const std::vector<nodeID_t>& nodeIDs) {
    KU_ASSERT(nextDenseFrontier);
    for (auto& nodeID : nodeIDs) {
        nextDenseFrontier[nodeID.offset].store(getCurrentIter(), std::memory_order_relaxed);
    }
}

iteration_t SPFrontierPair::getNextFrontierValue(offset_t offset) {
    KU_ASSERT(nextDenseFrontier);
    return nextDenseFrontier[offset].load(std::memory_order_relaxed);
}

offset_t SPFrontierPair::getNumActiveNodesInCurrentFrontier(
    NodeOffsetMaskMap& mask) {
    auto result = 0u;
    for (auto& [tableID, maxNumNodes] : pathLengths->getNodeMaxOffsetMap()) {
        pinCurrentFrontier(tableID);
        if (!mask.containsTableID(tableID)) {
            continue;
        }
        auto offsetMask = mask.getOffsetMask(tableID);
        for (auto offset = 0u; offset < maxNumNodes; ++offset) {
            if (isActive(offset)) {
                result += offsetMask->isMasked(offset);
            }
        }
    }
    return result;
}

bool SPFrontierPair::isActive(offset_t offset) {
    KU_ASSERT(curDenseFrontier);
    return curDenseFrontier[offset].load(std::memory_order_relaxed) == getCurrentIter() - 1;
}

void DoublePathLengthsFrontierPair::pinCurrentFrontier(table_id_t tableID) {
    curDenseFrontier->pinTableID(tableID);
}

void DoublePathLengthsFrontierPair::pinNextFrontier(table_id_t tableID) {
    nextDenseFrontier->pinTableID(tableID);
}

void DoublePathLengthsFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curDenseFrontier, nextDenseFrontier);
}

void DoublePathLengthsFrontierPair::addNodesToNextDenseFrontier(
    const std::vector<nodeID_t>& nodeIDs) {
    for (auto& nodeID : nodeIDs) {
        nextDenseFrontier->setIteration(nodeID.offset, getCurrentIter());
    }
}

void DoublePathLengthsFrontierPair::setCurFrontierValue(offset_t offset,
    iteration_t value) {
    curDenseFrontier->setIteration(offset, value);
}

void DoublePathLengthsFrontierPair::setNextFrontierValue(offset_t offset,
    iteration_t value) {
    nextDenseFrontier->setIteration(offset, value);
}

bool DoublePathLengthsFrontierPair::isActive(offset_t offset) {
    return curDenseFrontier->getIteration(offset) == getCurrentIter() - 1;
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
