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

void FrontierMorselDispatcher::init(common::offset_t _maxOffset) {
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

void SparseFrontier::pinTableID(common::table_id_t tableID) {
    curTableID = tableID;
    if (!tableIDToOffsetMap.contains(tableID)) {
        tableIDToOffsetMap.insert({tableID, std::unordered_set<common::offset_t>{}});
    }
    curOffsetSet = &tableIDToOffsetMap.at(tableID);
}

void SparseFrontier::addNode(common::offset_t offset) {
    std::unique_lock<std::mutex> lck{mtx};
    curOffsetSet->insert(offset);
}

void SparseFrontier::addNodes(const std::vector<common::nodeID_t> nodeIDs) {
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

PathLengths::PathLengths(const common::table_id_map_t<common::offset_t>& nodeMaxOffsetMap,
    storage::MemoryManager* mm)
    : nodeMaxOffsetMap{nodeMaxOffsetMap}, curIter{0} {
    for (const auto& [tableID, maxOffset] : nodeMaxOffsetMap) {
        auto memBuffer = mm->allocateBuffer(false, maxOffset * sizeof(frontier_entry_t));
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
    graph::Graph* graph, common::NodeOffsetMaskMap* maskMap) {
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
                frontier->setCurFrontierValue(i, INITIAL_VISITED);
            }
        } else {
            auto mask = maskMap->getOffsetMask(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                if (mask->isMasked(i)) {
                    frontier->setCurFrontierValue(i, INITIAL_VISITED);
                }
            }
        }
    }
    return frontier;
}

PathLengths::frontier_entry_t* PathLengths::getMaskData(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    return reinterpret_cast<frontier_entry_t*>(masks.at(tableID)->getData());
}

bool PathLengthsInitVertexCompute::beginOnTable(common::table_id_t tableID) {
    pathLengths.pinTableID(tableID);
    return true;
}

void PathLengthsInitVertexCompute::vertexCompute(common::offset_t startOffset,
    common::offset_t endOffset, common::table_id_t) {
    for (auto i = startOffset; i < endOffset; ++i) {
        pathLengths.setCurFrontierValue(i, val);
    }
}

FrontierPair::FrontierPair(std::shared_ptr<PathLengths> curFrontier,
    std::shared_ptr<PathLengths> nextFrontier)
    : curDenseFrontier{std::move(curFrontier)}, nextDenseFrontier{std::move(nextFrontier)} {
    initState();
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
    getCurDenseFrontier().resetCurIter();
    getNextDenseFrontier().resetCurIter();
}

void FrontierPair::beginFrontierComputeBetweenTables(common::table_id_t curTableID,
    common::table_id_t nextTableID) {
    pinCurrFrontier(curTableID);
    pinNextFrontier(nextTableID);
}

bool FrontierPair::continueNextIter(uint16_t maxIter) {
    return hasActiveNodesForNextIter_.load(std::memory_order_relaxed) && getCurrentIter() < maxIter;
}

void FrontierPair::addNodeToNextDenseFrontier(common::nodeID_t nodeID) {
    nextDenseFrontier->setActive(nodeID);
}

void FrontierPair::addNodesToNextDenseFrontier(const std::vector<common::nodeID_t>& nodeIDs) {
    nextDenseFrontier->setActive(nodeIDs);
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

void FrontierPair::initSource(common::nodeID_t source) {
    nextDenseFrontier->pinNextFrontierTableID(source.tableID);
    nextDenseFrontier->setActive(source);
    nextSparseFrontier->pinTableID(source.tableID);
    nextSparseFrontier->addNode(source.offset);
    hasActiveNodesForNextIter_.store(true);
}

void DoublePathLengthsFrontierPair::beginNewIterationInternalNoLock() {
    std::swap(curDenseFrontier, nextDenseFrontier);
    curDenseFrontier->incrementCurIter();
    nextDenseFrontier->incrementCurIter();
}

static constexpr uint64_t EARLY_TERM_NUM_NODES_THRESHOLD = 100;

bool SPEdgeCompute::terminate(common::NodeOffsetMaskMap& maskMap) {
    auto targetNumNodes = maskMap.getNumMaskedNode();
    if (targetNumNodes > EARLY_TERM_NUM_NODES_THRESHOLD) {
        // Skip checking if it's unlikely to early terminate.
        return false;
    }

    auto& frontier = frontierPair->getCurDenseFrontier();
    for (auto& [tableID, maxNumNodes] : frontier.getNodeMaxOffsetMap()) {
        frontier.pinTableID(tableID);
        if (!maskMap.containsTableID(tableID)) {
            continue;
        }
        auto offsetMask = maskMap.getOffsetMask(tableID);
        for (auto offset = 0u; offset < maxNumNodes; ++offset) {
            if (frontier.isActive(offset)) {
                numNodesReached += offsetMask->isMasked(offset);
            }
        }
    }
    return numNodesReached == targetNumNodes;
}

} // namespace function
} // namespace kuzu
