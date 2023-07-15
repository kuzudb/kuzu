#include "processor/operator/recursive_extend/bfs_scheduler.h"

#include "chrono"

namespace kuzu {
namespace processor {

bool MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* srcNodeIDVector, std::unique_ptr<BaseBFSMorsel>& bfsMorsel,
    std::pair<GlobalSSSPState, SSSPLocalState>& state) {
    std::unique_lock lck{mutex};
    switch (schedulerType) {
    case SchedulerType::OneThreadOneMorsel: {
        auto inputFTableMorsel = inputFTableSharedState->getMorsel();
        if (inputFTableMorsel->numTuples == 0) {
            state = {COMPLETE, NO_WORK_TO_SHARE};
            return true;
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        bfsMorsel->resetState();
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        bfsMorsel->markSrc(nodeID);
        return false;
    }
    case SchedulerType::nThreadkMorsel: {
        switch (globalState) {
        case COMPLETE: {
            state = {globalState, NO_WORK_TO_SHARE};
            return true;
        }
        case IN_PROGRESS_ALL_SRC_SCANNED: {
            return findAvailableSSSP(bfsMorsel, state);
        }
        case IN_PROGRESS: {
            if (numActiveSSSP < activeSSSPSharedState.size()) {
                auto inputFTableMorsel = inputFTableSharedState->getMorsel();
                if (inputFTableMorsel->numTuples == 0) {
                    globalState = (numActiveSSSP == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                    state = {globalState, NO_WORK_TO_SHARE};
                    return true;
                }
                numActiveSSSP++;
                auto newSSSPSharedState =
                    std::make_shared<SSSPSharedState>(upperBound, lowerBound, maxOffset);
                inputFTableSharedState->getTable()->scan(vectorsToScan,
                    inputFTableMorsel->startTupleIdx, inputFTableMorsel->numTuples,
                    colIndicesToScan);
                newSSSPSharedState->reset(bfsMorsel->targetDstNodes);
                newSSSPSharedState->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
                auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
                    srcNodeIDVector->state->selVector->selectedPositions[0]);
                newSSSPSharedState->srcOffset = nodeID.offset;
                newSSSPSharedState->markSrc(bfsMorsel->targetDstNodes->contains(nodeID));
                SSSPLocalState tempState{EXTEND_IN_PROGRESS};
                newSSSPSharedState->getBFSMorsel(bfsMorsel, tempState);
                for (auto i = 0u; i < activeSSSPSharedState.size(); i++) {
                    if (!activeSSSPSharedState[i]) {
                        newSSSPSharedState->pos = i;
                        activeSSSPSharedState[i] = newSSSPSharedState;
                        break;
                    }
                }
                return false;
            } else {
                return findAvailableSSSP(bfsMorsel, state);
            }
        }
        }
    }
    }
}

/**
 * Iterate over list of ALL currently active SSSPSharedStates and check if there is any work
 * available. NOTE: There can be different policies to decide which SSSPSharedState to help finish.
 * Right now the most basic policy has been implemented. The 1st SSSPSharedState we find that is
 * unfinished is chosen for helping work on.
 */
uint32_t MorselDispatcher::getNextAvailableSSSPWork() {
    for (auto i = 0u; i < activeSSSPSharedState.size(); i++) {
        if (activeSSSPSharedState[i]) {
            activeSSSPSharedState[i]->mutex.lock();
            if (activeSSSPSharedState[i]->hasWork()) {
                activeSSSPSharedState[i]->mutex.unlock();
                return i;
            }
            activeSSSPSharedState[i]->mutex.unlock();
        }
    }
    return UINT32_MAX;
}

/**
 * Try to find an available SSSPSharedState for work and then try to get a morsel. If the work is
 * writing path lengths then the tempState value will be PATH_LENGTH_WRITE_IN_PROGRESS.
 * We update the activeSSSPSharedState vector for the thread's index with the new
 * SSSPSharedState to indicate the morsel the thread will now be working on.
 *
 * @return - TRUE if no work was found OR work is writing pathLength values to vector.
 * FALSE if bfsExtension work was found i.e a range from current frontier to extend.
 */
bool MorselDispatcher::findAvailableSSSP(
    std::unique_ptr<BaseBFSMorsel>& bfsMorsel, std::pair<GlobalSSSPState, SSSPLocalState>& state) {
    SSSPLocalState tempState{NO_WORK_TO_SHARE};
    auto availableSSSPIdx = getNextAvailableSSSPWork();
    if (availableSSSPIdx == UINT32_MAX) {
        state = {globalState, tempState};
        return true;
    }
    bool checkState = activeSSSPSharedState[availableSSSPIdx]->getBFSMorsel(bfsMorsel, tempState);
    if (checkState) {
        state = {globalState, tempState};
        return checkState;
    } else {
        return false;
    }
}

/**
 * Return value = -1: New SSSPSharedState can be started.
 * Return value = 0, The pathLengthMorsel received did not yield any value to write to the
 * pathLengthVector.
 * Return value > 0, indicates pathLengths were written to pathLengthVector.
 */
int64_t MorselDispatcher::writeDstNodeIDAndPathLength(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::unique_ptr<BaseBFSMorsel>& baseBfsMorsel) {
    auto ssspSharedState = baseBfsMorsel->ssspSharedState;
    if (!ssspSharedState) {
        return -1;
    }
    auto startScanIdxAndSize = ssspSharedState->getDstPathLengthMorsel();
    /**
     * [startScanIdx, Size] = [UINT64_MAX, INT64_MAX] = no work in this morsel, exit & return -1
     * [startScanIdx, Size] = [0, -1] = SSSPSharedState was just completed (marked MORSEL_COMPLETE)
     * Else a pathLengthMorsel has been allotted to the thread and it can proceed to write the
     * path lengths to the pathLengthVector.
     */
    if (startScanIdxAndSize.first == UINT64_MAX && startScanIdxAndSize.second == INT64_MAX) {
        return -1;
    } else if (startScanIdxAndSize.second == -1) {
        mutex.lock();
        numActiveSSSP--;
        activeSSSPSharedState[ssspSharedState->pos] = nullptr;
        // If all SSSP have been completed indicated by count (numActiveSSSP == 0) and no more
        // SSSP are available to launched indicated by state IN_PROGRESS_ALL_SRC_SCANNED then
        // global state can be successfully changed to COMPLETE to indicate completion of SSSP
        // Computation.
        if (numActiveSSSP == 0 && globalState == IN_PROGRESS_ALL_SRC_SCANNED) {
            globalState = COMPLETE;
        }
        mutex.unlock();
        return -1;
    }
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx) {
        if (ssspSharedState->pathLength[startScanIdxAndSize.first] >= ssspSharedState->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            pathLengthVector->setValue<int64_t>(
                size, ssspSharedState->pathLength[startScanIdxAndSize.first]);
            size++;
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan,
            ssspSharedState->inputFTableTupleIdx, 1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
