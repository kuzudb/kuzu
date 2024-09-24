#include "function/gds/gds_frontier.h"
namespace kuzu {
namespace function {

void FrontierMorselDispatcher::init(table_id_t _tableID, common::offset_t _numOffsets) {
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

} // namespace function
} // namespace kuzu
