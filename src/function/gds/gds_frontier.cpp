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
    curTableID = tableID;
    if (!tableIDToOffsetMap.contains(tableID)) {
        tableIDToOffsetMap.insert({tableID, std::unordered_set<offset_t>{}});
    }
    curOffsetSet = &tableIDToOffsetMap.at(tableID);
}

void SparseFrontier::addNode(offset_t offset) {
    std::unique_lock<std::mutex> lck{mtx};
    curOffsetSet->insert(offset);
}

void SparseFrontier::addNodes(const std::vector<nodeID_t> nodeIDs) {
    if (!enabled_) {
        return;
    }
    KU_ASSERT(curOffsetSet != nullptr);
    for (auto nodeID : nodeIDs) {
        curOffsetSet->insert(nodeID.offset);
    }
}

void SparseFrontier::checkSampleSize() {
    if (!enabled_) {
        return;
    }
    enabled_ = curOffsetSet->size() < sampleSize;
}

void SparseFrontier::mergeLocalFrontier(const SparseFrontier& localFrontier) {
    std::unique_lock<std::mutex> lck{mtx};
    KU_ASSERT(localFrontier.enabled());
    for (auto& offset : localFrontier.getOffsetSet()) {
        curOffsetSet->insert(offset);
    }
}

void SparseFrontier::mergeSparseFrontier(const SparseFrontier& other) {
    std::unique_lock<std::mutex> lck{mtx};
    if (!enabled()) {
        return;
    }
    if (!other.enabled()) {
        disable();
        return;
    }
    for (auto [tableID, offsetSet] : other.tableIDToOffsetMap) {
        pinTableID(tableID);
        curOffsetSet->insert(offsetSet.begin(), offsetSet.end());
    }
}

PathLengths::PathLengths(const table_id_map_t<offset_t>& nodeMaxOffsetMap,
    storage::MemoryManager* mm)
    : nodeMaxOffsetMap{nodeMaxOffsetMap} {
    for (const auto& [tableID, maxOffset] : nodeMaxOffsetMap) {
        auto memBuffer = mm->allocateBuffer(false, maxOffset * sizeof(std::atomic<iteration_t>));
        masks.insert({tableID, std::move(memBuffer)});
    }
}

PathLengths::~PathLengths() = default;

std::shared_ptr<PathLengths> PathLengths::getUnvisitedFrontier(ExecutionContext* context,
    Graph* graph) {
    auto tx = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto frontier = std::make_shared<PathLengths>(graph->getMaxOffsetMap(tx), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*frontier, UNVISITED);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return frontier;
}

std::shared_ptr<PathLengths> PathLengths::getVisitedFrontier(processor::ExecutionContext* context,
    graph::Graph* graph) {
    auto tx = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto frontier = std::make_shared<PathLengths>(graph->getMaxOffsetMap(tx), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*frontier, INITIAL_VISITED);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return frontier;
}

std::shared_ptr<PathLengths> PathLengths::getVisitedFrontier(processor::ExecutionContext* context,
    graph::Graph* graph, NodeOffsetMaskMap* maskMap) {
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
    KU_ASSERT(masks.contains(tableID));
    return reinterpret_cast<std::atomic<iteration_t>*>(masks.at(tableID)->getData());
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
    vertexComputeCandidates->mergeSparseFrontier(*nextSparseFrontier);
    std::swap(curSparseFrontier, nextSparseFrontier);
    nextSparseFrontier->resetState();
    beginNewIterationInternalNoLock();
}

void FrontierPair::initGDS() {
    // This function should be called before beginNewIteration.
    // After beginNewIteration, next frontier will become current frontier.
    setActiveNodesForNextIter();
    // GDS initial frontier is usually large so we disable sparse frontier by default.
    nextSparseFrontier->disable();
}

void FrontierPair::initState() {
    curSparseFrontier = std::make_shared<SparseFrontier>();
    nextSparseFrontier = std::make_shared<SparseFrontier>();
    vertexComputeCandidates = std::make_shared<SparseFrontier>();
    hasActiveNodesForNextIter_.store(false);
    curIter.store(0u);
}

void FrontierPair::mergeLocalFrontier(const SparseFrontier& localFrontier) {
    std::unique_lock<std::mutex> lck{mtx};
    if (!nextSparseFrontier->enabled()) {
        return;
    }
    if (!localFrontier.enabled()) {
        nextSparseFrontier->disable();
        return;
    }
    nextSparseFrontier->mergeLocalFrontier(localFrontier);
}

bool FrontierPair::isCurFrontierSparse() {
    return curSparseFrontier->enabled();
}

void SinglePathLengthsFrontierPair::initSource(nodeID_t source) {
    pathLengths->pinTableID(source.tableID);
    pathLengths->setIteration(source.offset, getCurrentIter());
    nextSparseFrontier->pinTableID(source.tableID);
    nextSparseFrontier->addNode(source.offset);
    hasActiveNodesForNextIter_.store(true);
}

void SinglePathLengthsFrontierPair::pinCurrentFrontier(table_id_t tableID) {
    curSparseFrontier->pinTableID(tableID);
    curDenseFrontier = pathLengths->getMaskData(tableID);
}

void SinglePathLengthsFrontierPair::pinNextFrontier(table_id_t tableID) {
    nextSparseFrontier->pinTableID(tableID);
    nextDenseFrontier = pathLengths->getMaskData(tableID);
}

void SinglePathLengthsFrontierPair::beginNewIterationInternalNoLock() {
    // Do nothing
}

void SinglePathLengthsFrontierPair::addNodeToNextDenseFrontier(nodeID_t nodeID) {
    KU_ASSERT(nextDenseFrontier);
    nextDenseFrontier[nodeID.offset].store(getCurrentIter(), std::memory_order_relaxed);
}

void SinglePathLengthsFrontierPair::addNodesToNextDenseFrontier(
    const std::vector<nodeID_t>& nodeIDs) {
    KU_ASSERT(nextDenseFrontier);
    for (auto& nodeID : nodeIDs) {
        nextDenseFrontier[nodeID.offset].store(getCurrentIter(), std::memory_order_relaxed);
    }
}

iteration_t SinglePathLengthsFrontierPair::getNextFrontierValue(offset_t offset) {
    KU_ASSERT(nextDenseFrontier);
    return nextDenseFrontier[offset].load(std::memory_order_relaxed);
}

offset_t SinglePathLengthsFrontierPair::getNumActiveNodesInCurrentFrontier(
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

bool SinglePathLengthsFrontierPair::isActive(offset_t offset) {
    KU_ASSERT(curDenseFrontier);
    return curDenseFrontier[offset].load(std::memory_order_relaxed) == getCurrentIter() - 1;
}

void DoublePathLengthsFrontierPair::initSource(nodeID_t source) {
    nextDenseFrontier->pinTableID(source.tableID);
    nextDenseFrontier->setIteration(source.offset, getCurrentIter());
    nextSparseFrontier->pinTableID(source.tableID);
    nextSparseFrontier->addNode(source.offset);
    hasActiveNodesForNextIter_.store(true);
}

void DoublePathLengthsFrontierPair::pinCurrentFrontier(table_id_t tableID) {
    curSparseFrontier->pinTableID(tableID);
    curDenseFrontier->pinTableID(tableID);
}

void DoublePathLengthsFrontierPair::pinNextFrontier(table_id_t tableID) {
    nextSparseFrontier->pinTableID(tableID);
    nextDenseFrontier->pinTableID(tableID);
}

void DoublePathLengthsFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curDenseFrontier, nextDenseFrontier);
}

void DoublePathLengthsFrontierPair::addNodeToNextDenseFrontier(nodeID_t nodeID) {
    nextDenseFrontier->setIteration(nodeID.offset, getCurrentIter());
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

// TODO
offset_t DoublePathLengthsFrontierPair::getNumActiveNodesInCurrentFrontier(
    NodeOffsetMaskMap& mask) {
    throw RuntimeException("This should not be called.");
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
