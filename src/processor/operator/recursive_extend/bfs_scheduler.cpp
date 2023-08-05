#include "processor/operator/recursive_extend/bfs_scheduler.h"

#include "chrono"
#include <iostream>
#include <utility>

namespace kuzu {
namespace processor {

/**
 * Main function of the scheduler that dispatches morsels from the FTable between threads. Or it can
 * be called if a thread could not find a morsel from it's local BFSSharedState.
 *
 * @return - TRUE indicates check the combination value of the state pair, FALSE indicates some
 * work was found hence the state's values can be ignored.
 */
std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* srcNodeIDVector, BaseBFSMorsel* bfsMorsel,
    common::QueryRelType queryRelType) {
    std::unique_lock lck{mutex};
    switch (schedulerType) {
    case SchedulerType::OneThreadOneMorsel: {
        auto inputFTableMorsel = inputFTableSharedState->getMorsel();
        if (inputFTableMorsel->numTuples == 0) {
            return {COMPLETE, NO_WORK_TO_SHARE};
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        bfsMorsel->resetState();
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        bfsMorsel->markSrc(nodeID);
        return {IN_PROGRESS, EXTEND_IN_PROGRESS};
    }
    case SchedulerType::nThreadkMorsel: {
        switch (globalState) {
        case COMPLETE: {
            return {COMPLETE, NO_WORK_TO_SHARE};
        }
        case IN_PROGRESS_ALL_SRC_SCANNED: {
            return findAvailableSSSP(bfsMorsel);
        }
        case IN_PROGRESS: {
            if (numActiveSSSP < activeBFSSharedState.size()) {
                auto inputFTableMorsel = inputFTableSharedState->getMorsel();
                if (inputFTableMorsel->numTuples == 0) {
                    globalState = (numActiveSSSP == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                    return {globalState, NO_WORK_TO_SHARE};
                }
                numActiveSSSP++;
                inputFTableSharedState->getTable()->scan(vectorsToScan,
                    inputFTableMorsel->startTupleIdx, inputFTableMorsel->numTuples,
                    colIndicesToScan);
                auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
                    srcNodeIDVector->state->selVector->selectedPositions[0]);
                uint32_t newSharedStateIdx = UINT32_MAX;
                // Find a position for the new SSSP in the list, there are 2 candidates:
                // 1) the position has a nullptr, add the shared state there
                // 2) the SSSP is marked MORSEL_COMPLETE, it is complete and can be replaced
                for (int i = 0; i < activeBFSSharedState.size(); i++) {
                    if (!activeBFSSharedState[i]) {
                        newSharedStateIdx = i;
                        break;
                    }
                    activeBFSSharedState[i]->mutex.lock();
                    if (activeBFSSharedState[i]->isComplete()) {
                        newSharedStateIdx = i;
                        activeBFSSharedState[i]->mutex.unlock();
                        break;
                    }
                    activeBFSSharedState[i]->mutex.unlock();
                }
                if (newSharedStateIdx == UINT32_MAX || !activeBFSSharedState[newSharedStateIdx]) {
                    auto newBFSSharedState =
                        std::make_shared<BFSSharedState>(upperBound, lowerBound, maxOffset);
                    setUpNewBFSSharedState(newBFSSharedState, bfsMorsel, inputFTableMorsel.get(),
                        nodeID, queryRelType);
                    if (newSharedStateIdx != UINT32_MAX) {
                        activeBFSSharedState[newSharedStateIdx] = newBFSSharedState;
                    }
                } else {
                    activeBFSSharedState[newSharedStateIdx]->mutex.lock();
                    setUpNewBFSSharedState(activeBFSSharedState[newSharedStateIdx], bfsMorsel,
                        inputFTableMorsel.get(), nodeID, queryRelType);
                    activeBFSSharedState[newSharedStateIdx]->mutex.unlock();
                }
                return {IN_PROGRESS, EXTEND_IN_PROGRESS};
            } else {
                return findAvailableSSSP(bfsMorsel);
            }
        }
        }
    }
    }
}

void MorselDispatcher::setUpNewBFSSharedState(std::shared_ptr<BFSSharedState>& newBFSSharedState,
    BaseBFSMorsel* bfsMorsel, FactorizedTableScanMorsel* inputFTableMorsel, common::nodeID_t nodeID,
    common::QueryRelType queryRelType) {
    newBFSSharedState->reset(bfsMorsel->targetDstNodes, queryRelType);
    newBFSSharedState->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
    newBFSSharedState->srcOffset = nodeID.offset;
    newBFSSharedState->markSrc(bfsMorsel->targetDstNodes->contains(nodeID), queryRelType);
    newBFSSharedState->numThreadsActiveOnMorsel++;
    auto bfsMorselSize = std::min(common::DEFAULT_VECTOR_CAPACITY,
        newBFSSharedState->bfsLevelNodeOffsets.size() - newBFSSharedState->nextScanStartIdx);
    auto morselScanEndIdx = newBFSSharedState->nextScanStartIdx + bfsMorselSize;
    bfsMorsel->reset(
        newBFSSharedState->nextScanStartIdx, morselScanEndIdx, newBFSSharedState.get());
    newBFSSharedState->nextScanStartIdx += bfsMorselSize;
}

uint32_t MorselDispatcher::getNextAvailableSSSPWork() {
    for (auto i = 0u; i < activeBFSSharedState.size(); i++) {
        if (activeBFSSharedState[i]) {
            activeBFSSharedState[i]->mutex.lock();
            if (activeBFSSharedState[i]->hasWork()) {
                activeBFSSharedState[i]->mutex.unlock();
                return i;
            }
            activeBFSSharedState[i]->mutex.unlock();
        }
    }
    return UINT32_MAX;
}

/**
 * Try to find an available BFSSharedState for work and then try to get a morsel. If the work is
 * writing path lengths then the tempState value will be PATH_LENGTH_WRITE_IN_PROGRESS.
 *
 * @return - TRUE if no work was found OR work is writing pathLength values to vector.
 * FALSE if bfsExtension work was found i.e a range from current frontier to extend.
 */
std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::findAvailableSSSP(
    BaseBFSMorsel* bfsMorsel) {
    std::pair<GlobalSSSPState, SSSPLocalState> state{globalState, NO_WORK_TO_SHARE};
    auto availableSSSPIdx = getNextAvailableSSSPWork();
    if (availableSSSPIdx == UINT32_MAX) {
        return state;
    }
    state.second = activeBFSSharedState[availableSSSPIdx]->getBFSMorsel(bfsMorsel);
    return state;
}

/**
 * Return value = -1: New BFSSharedState can be started.
 * Return value = 0, The pathLengthMorsel received did not yield any value to write to the
 * pathLengthVector.
 * Return value > 0, indicates pathLengths were written to pathLengthVector.
 */
int64_t MorselDispatcher::writeDstNodeIDAndPathLength(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::unique_ptr<BaseBFSMorsel>& baseBfsMorsel) {
    if (!baseBfsMorsel->hasBFSSharedState()) {
        return -1;
    }
    if (baseBfsMorsel->hasMoreToWrite()) {
        auto startScanIdxAndSize = baseBfsMorsel->getPrevDistStartScanIdxAndSize();
        return baseBfsMorsel->writeToVector(inputFTableSharedState, std::move(vectorsToScan),
            std::move(colIndicesToScan), dstNodeIDVector, pathLengthVector, tableID,
            startScanIdxAndSize);
    }
    auto bfsSharedState = baseBfsMorsel->bfsSharedState;
    auto startScanIdxAndSize = bfsSharedState->getDstPathLengthMorsel();
    /**
     * [startScanIdx, Size] = [UINT64_MAX, INT64_MAX] = no work in this morsel, exit & return -1
     * [startScanIdx, Size] = [0, -1] = BFSSharedState was just completed (marked MORSEL_COMPLETE)
     * Else a pathLengthMorsel has been allotted to the thread and it can proceed to write the
     * path lengths to the pathLengthVector.
     */
    if (startScanIdxAndSize.first == UINT64_MAX && startScanIdxAndSize.second == INT64_MAX) {
        return -1;
    }
    if (startScanIdxAndSize.second == -1) {
        mutex.lock();
        bfsSharedState->mutex.lock();
        numActiveSSSP--;
        bfsSharedState->ssspLocalState = MORSEL_COMPLETE;
        bfsSharedState->mutex.unlock();
        // If all SSSP have been completed indicated by count (numActiveSSSP == 0) and no more
        // SSSP are available indicated by state IN_PROGRESS_ALL_SRC_SCANNED then global state can
        // be successfully changed to COMPLETE to indicate completion of SSSP Computation.
        if (numActiveSSSP == 0 && globalState == IN_PROGRESS_ALL_SRC_SCANNED) {
            globalState = COMPLETE;
        }
        mutex.unlock();
        return -1;
    }
    return baseBfsMorsel->writeToVector(inputFTableSharedState, std::move(vectorsToScan),
        std::move(colIndicesToScan), dstNodeIDVector, pathLengthVector, tableID,
        startScanIdxAndSize);
}

} // namespace processor
} // namespace kuzu
