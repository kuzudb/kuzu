#if defined(_MSC_VER)
#include <Windows.h>
#include <intrin.h>
#endif
#include "common/exception.h"
#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/bfs_state.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

void BFSSharedState::reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType) {
    ssspLocalState = EXTEND_IN_PROGRESS;
    currentLevel = 0u;
    nextScanStartIdx = 0u;
    numVisitedNodes = 0u;
    auto totalDestinations = targetDstNodes->getNumNodes();
    if (totalDestinations == (maxOffset + 1) || totalDestinations == 0u) {
        // All node offsets are destinations hence mark all as not visited destinations.
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED);
        for (auto& dstOffset : targetDstNodes->getNodeIDs()) {
            visitedNodes[dstOffset.offset] = NOT_VISITED_DST;
        }
    }
    std::fill(pathLength.begin(), pathLength.end(), 0u);
    bfsLevelNodeOffsets.clear();
    srcOffset = 0u;
    numThreadsBFSActive = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    pathLengthThreadWriters = std::unordered_set<std::thread::id>();
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        minDistance = 0u;
        // nodeIDToMultiplicity is not defined in the constructor directly, only for all shortest
        // recursive join it is required. If it is empty then assign a vector of size maxNodeOffset
        // to it (same size as visitedNodes).
        if (nodeIDToMultiplicity.empty()) {
            nodeIDToMultiplicity = std::vector<uint64_t>(visitedNodes.size(), 0u);
        } else {
            std::fill(nodeIDToMultiplicity.begin(), nodeIDToMultiplicity.end(), 0u);
        }
    }
}

/*
 * Returning the state here because if BFSSharedState is complete / in pathLength writing stage
 * then depending on state we need to take next step. If MORSEL_COMPLETE then proceed to get a new
 * BFSSharedState & if MORSEL_pathLength_WRITING_IN_PROGRESS then help in this task.
 */
SSSPLocalState BFSSharedState::getBFSMorsel(BaseBFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    switch (ssspLocalState) {
    case MORSEL_COMPLETE: {
        return NO_WORK_TO_SHARE;
    }
    case PATH_LENGTH_WRITE_IN_PROGRESS: {
        bfsMorsel->bfsSharedState = this;
        return PATH_LENGTH_WRITE_IN_PROGRESS;
    }
    case EXTEND_IN_PROGRESS: {
        if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
            numThreadsBFSActive++;
            auto bfsMorselSize = std::min(
                common::DEFAULT_VECTOR_CAPACITY, bfsLevelNodeOffsets.size() - nextScanStartIdx);
            auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
            bfsMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
            nextScanStartIdx += bfsMorselSize;
            return EXTEND_IN_PROGRESS;
        } else {
            return NO_WORK_TO_SHARE;
        }
    }
    default:
        throw common::RuntimeException(
            &"Unknown local state encountered inside BFSSharedState: "[ssspLocalState]);
    }
}

bool BFSSharedState::hasWork() const {
    if (ssspLocalState == EXTEND_IN_PROGRESS && nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        return true;
    }
    if (ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS &&
        nextDstScanStartIdx < visitedNodes.size()) {
        return true;
    }
    return false;
}

bool BFSSharedState::finishBFSMorsel(BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType) {
    std::unique_lock lck{mutex};
    numThreadsBFSActive--;
    if (ssspLocalState != EXTEND_IN_PROGRESS) {
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    // ONLY for shortest path and all shortest path recursive join.
    if (queryRelType == common::QueryRelType::SHORTEST) {
        auto shortestPathMorsel = (reinterpret_cast<ShortestPathMorsel<false>*>(bfsMorsel));
        numVisitedNodes += shortestPathMorsel->getNumVisitedDstNodes();
    } else if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        auto allShortestPathMorsel = (reinterpret_cast<AllShortestPathMorsel<false>*>(bfsMorsel));
        numVisitedNodes += allShortestPathMorsel->getNumVisitedDstNodes();
    }
    if (numThreadsBFSActive == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        moveNextLevelAsCurrentLevel();
        if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            return true;
        }
    } else if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
        ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
        return true;
    }
    return false;
}

bool BFSSharedState::isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType) {
    if (bfsLevelNodeOffsets.empty()) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (queryRelType == common::QueryRelType::SHORTEST) {
        return numVisitedNodes == numDstNodesToVisit;
    }
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        return (numVisitedNodes == numDstNodesToVisit) && (currentLevel > minDistance);
    }
    return false;
}

void BFSSharedState::markSrc(bool isSrcDestination, common::QueryRelType queryRelType) {
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        pathLength[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    bfsLevelNodeOffsets.push_back(srcOffset);
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        nodeIDToMultiplicity[srcOffset] = 1;
    }
}

void BFSSharedState::moveNextLevelAsCurrentLevel() {
    currentLevel++;
    nextScanStartIdx = 0u;
    bfsLevelNodeOffsets.clear();
    if (currentLevel < upperBound) { // No need to prepare this vector if we won't extend.
        for (auto i = 0u; i < visitedNodes.size(); i++) {
            if (visitedNodes[i] == VISITED_NEW) {
                visitedNodes[i] = VISITED;
                bfsLevelNodeOffsets.push_back(i);
            } else if (visitedNodes[i] == VISITED_DST_NEW) {
                visitedNodes[i] = VISITED_DST;
                bfsLevelNodeOffsets.push_back(i);
            }
        }
    }
}

std::pair<uint64_t, int64_t> BFSSharedState::getDstPathLengthMorsel() {
    std::unique_lock lck{mutex};
    if (ssspLocalState != PATH_LENGTH_WRITE_IN_PROGRESS) {
        return {UINT64_MAX, INT64_MAX};
    }
    auto threadID = std::this_thread::get_id();
    if (nextDstScanStartIdx == visitedNodes.size()) {
        if (!canThreadCompleteSharedState(threadID)) {
            return {UINT64_MAX, INT64_MAX};
        }
        pathLengthThreadWriters.erase(threadID);
        /// Last Thread to exit will be responsible for doing state change to MORSEL_COMPLETE
        /// Along with state change it will also decrement numActiveBFSSharedState by 1
        if (pathLengthThreadWriters.empty()) {
            return {0, -1};
        }
        return {UINT64_MAX, INT64_MAX};
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    pathLengthThreadWriters.insert(threadID);
    return startScanIdxAndSize;
}

template<>
void ShortestPathMorsel<false>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
#if defined(_MSC_VER)
            bool casResult =
                _InterlockedCompareExchange64(reinterpret_cast<volatile __int64*>(
                                                  &bfsSharedState->visitedNodes[nodeID.offset]),
                    VISITED_DST_NEW, state) == state;
#else
            bool casResult = __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW);
#endif
            if (casResult) {
                __sync_bool_compare_and_swap(&bfsSharedState->pathLength[nodeID.offset], 0u,
                    bfsSharedState->currentLevel + 1);
                numVisitedDstNodes++;
                printf("visited destination: %lu | at length: %u | current bfs level: %u\n",
                    nodeID.offset, bfsSharedState->pathLength[nodeID.offset],
                    bfsSharedState->currentLevel);
            }
        } else if (state == NOT_VISITED) {
#if defined(_MSC_VER)
            _InterlockedCompareExchange64(
                reinterpret_cast<volatile __int64*>(&bfsSharedState->visitedNodes[nodeID.offset]),
                VISITED_NEW, state);
#else
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
#endif
        }
    }
}

template<>
void ShortestPathMorsel<true>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {}

template<>
int64_t ShortestPathMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx) {
        if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
             bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
            bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            pathLengthVector->setValue<int64_t>(
                size, bfsSharedState->pathLength[startScanIdxAndSize.first]);
            size++;
            printf("writing for offset: %lu, length value: %u\n", startScanIdxAndSize.first,
                bfsSharedState->pathLength[startScanIdxAndSize.first]);
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

template<>
int64_t ShortestPathMorsel<true>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    throw common::NotImplementedException("Not implemented for TRACK_PATH and nTkS scheduler. ");
}

} // namespace processor
} // namespace kuzu
