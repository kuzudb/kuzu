#include "processor/operator/recursive_extend/bfs_scheduler.h"

#include "chrono"

namespace kuzu {
namespace processor {

/**
 * Populate the active SSSP tracker for each thread, and return a unique thread ID.
 * activeSSSPMorsel already has size equal to total threads.
 */
uint32_t MorselDispatcher::getThreadIdx() {
    std::unique_lock lck{mutex};
    activeSSSPMorsel[threadIdxCounter] = nullptr;
    return threadIdxCounter++;
}

/**
 *
 */
std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* srcNodeIDVector, std::unique_ptr<BaseBFSMorsel>& bfsMorsel,
    uint32_t threadIdx) {
    std::unique_lock lck{mutex};
    switch (schedulerType) {
    case SchedulerType::OneThreadOneMorsel: {
        auto inputFTableMorsel = inputFTableSharedState->getMorsel();
        if (inputFTableMorsel->numTuples == 0) {
            bfsMorsel->threadCheckSSSPState = true;
            return {COMPLETE, MORSEL_COMPLETE};
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        bfsMorsel->resetState();
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        bfsMorsel->markSrc(nodeID);
        bfsMorsel->threadCheckSSSPState = false;
        return {IN_PROGRESS, EXTEND_IN_PROGRESS};
    }
    case SchedulerType::nThreadkMorsel: {
        switch (globalState) {
        case COMPLETE: {
            bfsMorsel->threadCheckSSSPState = true;
            return {globalState, MORSEL_COMPLETE};
        }
        case IN_PROGRESS_ALL_SRC_SCANNED: {
            auto ssspMorsel = activeSSSPMorsel[threadIdx];
            SSSPLocalState ssspLocalState;
            if (!ssspMorsel) {
                bfsMorsel->threadCheckSSSPState = true;
                ssspLocalState = EXTEND_IN_PROGRESS;
            } else {
                ssspLocalState = ssspMorsel->getBFSMorsel(bfsMorsel);
            }
            if (bfsMorsel->threadCheckSSSPState) {
                switch (ssspLocalState) {
                    // try to get the next available SSSPMorsel for work
                    // same for both states, try to get next available SSSPMorsel for work
                case MORSEL_COMPLETE:
                case EXTEND_IN_PROGRESS: {
                    return findAvailableSSSPMorsel(bfsMorsel, ssspLocalState, threadIdx);
                }
                case PATH_LENGTH_WRITE_IN_PROGRESS:
                    bfsMorsel->threadCheckSSSPState = true;
                    return {globalState, ssspLocalState};
                default:
                    assert(false);
                }
            } else {
                return {globalState, ssspLocalState};
            }
        }
        case IN_PROGRESS: {
            auto ssspMorsel = activeSSSPMorsel[threadIdx];
            SSSPLocalState ssspLocalState;
            if (!ssspMorsel) {
                bfsMorsel->threadCheckSSSPState = true;
                ssspLocalState = EXTEND_IN_PROGRESS;
            } else {
                ssspLocalState = ssspMorsel->getBFSMorsel(bfsMorsel);
            }
            if (bfsMorsel->threadCheckSSSPState) {
                switch (ssspLocalState) {
                    // try to get the next available SSSPMorsel for work
                    // same for both states, try to get next available SSSPMorsel for work
                case MORSEL_COMPLETE:
                case EXTEND_IN_PROGRESS: {
                    if (numActiveSSSP < activeSSSPMorsel.size()) {
                        auto inputFTableMorsel = inputFTableSharedState->getMorsel();
                        if (inputFTableMorsel->numTuples == 0) {
                            globalState =
                                (numActiveSSSP == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                            bfsMorsel->threadCheckSSSPState = true;
                            return {globalState, ssspLocalState};
                        }
                        numActiveSSSP++;
                        auto newSSSPMorsel =
                            std::make_shared<SSSPMorsel>(upperBound, lowerBound, maxOffset);
                        auto duration = std::chrono::system_clock::now().time_since_epoch();
                        auto millis =
                            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                        inputFTableSharedState->getTable()->scan(vectorsToScan,
                            inputFTableMorsel->startTupleIdx, inputFTableMorsel->numTuples,
                            colIndicesToScan);
                        newSSSPMorsel->reset(bfsMorsel->targetDstNodes);
                        newSSSPMorsel->startTimeInMillis = millis;
                        newSSSPMorsel->lvlStartTimeInMillis = millis;
                        newSSSPMorsel->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
                        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
                            srcNodeIDVector->state->selVector->selectedPositions[0]);
                        newSSSPMorsel->srcOffset = nodeID.offset;
                        newSSSPMorsel->markSrc(bfsMorsel->targetDstNodes->contains(nodeID));
                        newSSSPMorsel->getBFSMorsel(bfsMorsel);
                        if (ssspMorsel) {
                            ssspMorsel->mutex.lock();
                            activeSSSPMorsel[threadIdx] = newSSSPMorsel;
                            ssspMorsel->mutex.unlock();
                        } else {
                            activeSSSPMorsel[threadIdx] = newSSSPMorsel;
                        }
                        return {globalState, ssspLocalState};
                    } else {
                        return findAvailableSSSPMorsel(bfsMorsel, ssspLocalState, threadIdx);
                    }
                }
                case PATH_LENGTH_WRITE_IN_PROGRESS:
                    bfsMorsel->threadCheckSSSPState = true;
                    return {globalState, ssspLocalState};
                default:
                    assert(false);
                }
            } else {
                return {globalState, ssspLocalState};
            }
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

int64_t MorselDispatcher::getNextAvailableSSSPWork() {
    auto nextAvailableSSSPIdx = -1;
    for (int i = 0; i < activeSSSPMorsel.size(); i++) {
        if (activeSSSPMorsel[i]) {
            activeSSSPMorsel[i]->mutex.lock();
            if (activeSSSPMorsel[i]->nextScanStartIdx <
                    activeSSSPMorsel[i]->bfsLevelNodeOffsets.size() &&
                (activeSSSPMorsel[i]->ssspLocalState == EXTEND_IN_PROGRESS ||
                    activeSSSPMorsel[i]->ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS)) {
                nextAvailableSSSPIdx = i;
                activeSSSPMorsel[i]->mutex.unlock();
                break;
            }
            activeSSSPMorsel[i]->mutex.unlock();
        }
    }
    return nextAvailableSSSPIdx;
}

/**
 *
 */
std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::findAvailableSSSPMorsel(
    std::unique_ptr<BaseBFSMorsel>& bfsMorsel, SSSPLocalState& ssspLocalState, uint32_t threadIdx) {
    auto nextAvailableSSSPIdx = getNextAvailableSSSPWork();
    if (nextAvailableSSSPIdx == -1) {
        bfsMorsel->threadCheckSSSPState = true;
        return {globalState, ssspLocalState};
    }
    ssspLocalState = activeSSSPMorsel[nextAvailableSSSPIdx]->getBFSMorsel(bfsMorsel);
    if (bfsMorsel->threadCheckSSSPState) {
        bfsMorsel->threadCheckSSSPState = true;
        return {globalState, ssspLocalState};
    } else {
        if (bfsMorsel->ssspMorsel) {
            bfsMorsel->ssspMorsel->mutex.lock();
            activeSSSPMorsel[threadIdx] = activeSSSPMorsel[nextAvailableSSSPIdx];
            bfsMorsel->ssspMorsel->mutex.unlock();
        } else {
            activeSSSPMorsel[threadIdx] = activeSSSPMorsel[nextAvailableSSSPIdx];
        }
        return {globalState, ssspLocalState};
    }
}

/**
 * Return value = -1: New SSSPMorsel can be started, current morsel's path length writing is done.
 * Return value = 0, The pathLengthMorsel received did not yield any value to write to the
 * pathLengthVector.
 * Return value > 0, indicates pathLengths were written to pathLengthVector.
 */
int64_t MorselDispatcher::writeDstNodeIDAndPathLength(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, uint32_t threadIdx) {
    mutex.lock();
    auto ssspMorsel = activeSSSPMorsel[threadIdx];
    if (!ssspMorsel) {
        mutex.unlock();
        return -1;
    }
    auto startScanIdxAndSize = ssspMorsel->getDstPathLengthMorsel();
    if (startScanIdxAndSize.first == UINT64_MAX && startScanIdxAndSize.second == INT64_MAX) {
        mutex.unlock();
        return -1;
    } else if (startScanIdxAndSize.second == -1) {
        numActiveSSSP--;
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
    mutex.unlock();
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx) {
        if (ssspMorsel->pathLength[startScanIdxAndSize.first] >= ssspMorsel->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            pathLengthVector->setValue<int64_t>(
                size, ssspMorsel->pathLength[startScanIdxAndSize.first]);
            size++;
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(
            vectorsToScan, ssspMorsel->inputFTableTupleIdx, 1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
