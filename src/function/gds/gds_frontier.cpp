#include "function/gds/gds_frontier.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

FrontierMorselDispatcher::FrontierMorselDispatcher(uint64_t _maxThreadsForExec)
    : morselSize(UINT64_MAX) {
    maxThreadsForExec.store(_maxThreadsForExec);
    tableID.store(INVALID_TABLE_ID);
    numOffsets.store(INVALID_OFFSET);
    nextOffset.store(INVALID_OFFSET);
}

void FrontierMorselDispatcher::init(common::table_id_t _tableID, common::offset_t _numOffsets) {
    tableID.store(_tableID);
    numOffsets.store(_numOffsets);
    nextOffset.store(0u);
    // Frontier size calculation: The ideal scenario is to have k^2 many morsels where k
    // the number of maximum threads that could be working on this frontier. However if
    // that is too small then we default to MIN_FRONTIER_MORSEL_SIZE.
    auto idealMorselSize = numOffsets.load(std::memory_order_relaxed) /
                           (std::max(MIN_NUMBER_OF_FRONTIER_MORSELS,
                               maxThreadsForExec.load(std::memory_order_relaxed) *
                                   maxThreadsForExec.load(std::memory_order_relaxed)));
    morselSize = std::max(MIN_FRONTIER_MORSEL_SIZE, idealMorselSize);
}

bool FrontierMorselDispatcher::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    auto beginOffset = nextOffset.fetch_add(morselSize, std::memory_order_acq_rel);
    if (beginOffset >= numOffsets.load(std::memory_order_relaxed)) {
        return false;
    }
    auto endOffsetExclusive =
        beginOffset + morselSize > numOffsets.load(std::memory_order_relaxed) ?
            numOffsets.load(std::memory_order_relaxed) :
            beginOffset + morselSize;
    frontierMorsel.initMorsel(tableID.load(std::memory_order_relaxed), beginOffset,
        endOffsetExclusive);
    return true;
}

PathLengths::PathLengths(const common::table_id_map_t<common::offset_t>& numNodesMap_,
    storage::MemoryManager* mm)
    : GDSFrontier{numNodesMap_} {
    curIter.store(0);
    for (const auto& [tableID, numNodes] : numNodesMap_) {
        auto memBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint16_t>));
        std::atomic<uint16_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint16_t>*>(memBuffer.get()->getData());
        for (uint64_t i = 0; i < numNodes; ++i) {
            memBufferPtr[i].store(UNVISITED, std::memory_order_relaxed);
        }
        masks.insert({tableID, std::move(memBuffer)});
    }
}

void PathLengths::fixCurFrontierNodeTable(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    curTableID.store(tableID, std::memory_order_relaxed);
    curFrontierFixedMask.store(
        reinterpret_cast<std::atomic<uint16_t>*>(masks.at(tableID).get()->getData()),
        std::memory_order_relaxed);
    maxNodesInCurFrontierFixedMask.store(numNodesMap[curTableID.load(std::memory_order_relaxed)],
        std::memory_order_relaxed);
}

void PathLengths::fixNextFrontierNodeTable(common::table_id_t tableID) {
    KU_ASSERT(masks.contains(tableID));
    nextFrontierFixedMask.store(
        reinterpret_cast<std::atomic<uint16_t>*>(masks.at(tableID).get()->getData()),
        std::memory_order_relaxed);
}

FrontierPair::FrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
    std::shared_ptr<GDSFrontier> nextFrontier, uint64_t initialActiveNodes,
    uint64_t maxThreadsForExec)
    : curFrontier{curFrontier}, nextFrontier{nextFrontier}, maxThreadsForExec{maxThreadsForExec} {
    numApproxActiveNodesForCurIter.store(UINT64_MAX);
    numApproxActiveNodesForNextIter.store(initialActiveNodes);
    curIter.store(0u);
}

void FrontierPair::beginNewIteration() {
    std::unique_lock<std::mutex> lck{mtx};
    curIter.fetch_add(1u);
    numApproxActiveNodesForCurIter.store(numApproxActiveNodesForNextIter.load());
    numApproxActiveNodesForNextIter.store(0u);
    std::swap(curFrontier, nextFrontier);
    beginNewIterationInternalNoLock();
}

void SinglePathLengthsFrontierPair::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    pathLengths->fixCurFrontierNodeTable(curFrontierTableID);
    pathLengths->fixNextFrontierNodeTable(nextFrontierTableID);
    morselDispatcher.init(curFrontierTableID,
        pathLengths->getNumNodesInCurFrontierFixedNodeTable());
}

bool SinglePathLengthsFrontierPair::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselDispatcher.getNextRangeMorsel(frontierMorsel);
}

void SinglePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    pathLengths->fixNextFrontierNodeTable(source.tableID);
    pathLengths->setActive(source);
}

DoublePathLengthsFrontierPair::DoublePathLengthsFrontierPair(
    common::table_id_map_t<common::offset_t> numNodesMap, uint64_t maxThreadsForExec,
    storage::MemoryManager* mm)
    : FrontierPair(std::make_shared<PathLengths>(numNodesMap, mm),
          std::make_shared<PathLengths>(numNodesMap, mm), 1 /* initial num active nodes */,
          maxThreadsForExec) {
    morselDispatcher = std::make_unique<FrontierMorselDispatcher>(maxThreadsForExec);
}

bool DoublePathLengthsFrontierPair::getNextRangeMorsel(FrontierMorsel& frontierMorsel) {
    return morselDispatcher->getNextRangeMorsel(frontierMorsel);
}

void DoublePathLengthsFrontierPair::beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
    table_id_t nextFrontierTableID) {
    curFrontier->ptrCast<PathLengths>()->fixCurFrontierNodeTable(curFrontierTableID);
    nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(nextFrontierTableID);
    morselDispatcher->init(curFrontierTableID,
        curFrontier->ptrCast<PathLengths>()->getNumNodesInCurFrontierFixedNodeTable());
}

void DoublePathLengthsFrontierPair::initRJFromSource(nodeID_t source) {
    nextFrontier->ptrCast<PathLengths>()->fixNextFrontierNodeTable(source.tableID);
    nextFrontier->ptrCast<PathLengths>()->setActive(source);
}

static constexpr uint64_t EARLY_TERM_NUM_NODES_THRESHOLD = 100;

bool SPEdgeCompute::terminate(processor::NodeOffsetMaskMap& maskMap) {
    auto targetNumNodes = maskMap.getNumMaskedNode();
    if (targetNumNodes > EARLY_TERM_NUM_NODES_THRESHOLD) {
        // Skip checking if it's unlikely to early terminate.
        return false;
    }
    auto& frontier = frontierPair->getCurrentFrontierUnsafe();
    for (auto& [tableID, maxNumNodes] : frontier.getNumNodesMap()) {
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
