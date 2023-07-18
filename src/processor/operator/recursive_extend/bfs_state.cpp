#if defined(_MSC_VER)
#include <Windows.h>
#include <intrin.h>
#endif
#include "common/exception.h"
#include "processor/operator/recursive_extend/bfs_state.h"

namespace kuzu {
namespace processor {

void SSSPSharedState::reset(TargetDstNodes* targetDstNodes) {
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
    numThreadsActiveOnMorsel = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
}

/*
 * Returning the state here because if SSSPSharedState is complete / in pathLength writing stage
 * then depending on state we need to take next step. If MORSEL_COMPLETE then proceed to get a new
 * SSSPSharedState & if MORSEL_pathLength_WRITING_IN_PROGRESS then help in this task.
 */
SSSPLocalState SSSPSharedState::getBFSMorsel(BaseBFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    switch (ssspLocalState) {
    case MORSEL_COMPLETE: {
        return NO_WORK_TO_SHARE;
    }
    case PATH_LENGTH_WRITE_IN_PROGRESS: {
        bfsMorsel->ssspSharedState = this;
        return PATH_LENGTH_WRITE_IN_PROGRESS;
    }
    case EXTEND_IN_PROGRESS: {
        if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
            numThreadsActiveOnMorsel++;
            auto bfsMorselSize = std::min(
                common::DEFAULT_VECTOR_CAPACITY, bfsLevelNodeOffsets.size() - nextScanStartIdx);
            auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
            auto shortestPathMorsel = (reinterpret_cast<ShortestPathMorsel<false>*>(bfsMorsel));
            shortestPathMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
            nextScanStartIdx += bfsMorselSize;
            return EXTEND_IN_PROGRESS;
        } else {
            return NO_WORK_TO_SHARE;
        }
    }
    default:
        throw common::RuntimeException(
            &"Unknown local state encountered inside SSSPSharedState: "[ssspLocalState]);
    }
}

bool SSSPSharedState::hasWork() const {
    if (ssspLocalState == EXTEND_IN_PROGRESS && nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        return true;
    }
    if (ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS &&
        nextDstScanStartIdx < visitedNodes.size()) {
        return true;
    }
    return false;
}

bool SSSPSharedState::finishBFSMorsel(BaseBFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    numThreadsActiveOnMorsel--;
    if (ssspLocalState != EXTEND_IN_PROGRESS) {
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    auto shortestPathMorsel = (reinterpret_cast<ShortestPathMorsel<false>*>(bfsMorsel));
    numVisitedNodes += shortestPathMorsel->getNumVisitedDstNodes();
    if (numThreadsActiveOnMorsel == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        moveNextLevelAsCurrentLevel();
        if (isComplete(shortestPathMorsel->targetDstNodes->getNumNodes())) {
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            return true;
        }
    } else if (isComplete(shortestPathMorsel->targetDstNodes->getNumNodes())) {
        ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
        return true;
    }
    return false;
}

bool SSSPSharedState::isComplete(uint64_t numDstNodesToVisit) {
    if (bfsLevelNodeOffsets.empty()) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == numDstNodesToVisit) { // all destinations have been reached.
        return true;
    }
    return false;
}

void SSSPSharedState::markSrc(bool isSrcDestination) {
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        pathLength[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    bfsLevelNodeOffsets.push_back(srcOffset);
}

void SSSPSharedState::moveNextLevelAsCurrentLevel() {
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

std::pair<uint64_t, int64_t> SSSPSharedState::getDstPathLengthMorsel() {
    std::unique_lock lck{mutex};
    if (ssspLocalState != PATH_LENGTH_WRITE_IN_PROGRESS) {
        return {UINT64_MAX, INT64_MAX};
    } else if (nextDstScanStartIdx == visitedNodes.size()) {
        ssspLocalState = MORSEL_COMPLETE;
        return {0, -1};
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    return startScanIdxAndSize;
}

template<>
void ShortestPathMorsel<false>::addToLocalNextBFSLevel(common::ValueVector* tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = ssspSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
#if defined(_MSC_VER)
            bool casResult =
                _InterlockedCompareExchange64(reinterpret_cast<volatile __int64*>(
                                                  &ssspSharedState->visitedNodes[nodeID.offset]),
                    VISITED_DST_NEW, state) == state;
#else
            bool casResult = __sync_bool_compare_and_swap(
                &ssspSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW);
#endif
            if (casResult) {
                ssspSharedState->pathLength[nodeID.offset] = ssspSharedState->currentLevel + 1;
                numVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
#if defined(_MSC_VER)
            _InterlockedCompareExchange64(
                reinterpret_cast<volatile __int64*>(&ssspSharedState->visitedNodes[nodeID.offset]),
                VISITED_NEW, state);
#else
            __sync_bool_compare_and_swap(
                &ssspSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
#endif
        }
    }
}

} // namespace processor
} // namespace kuzu
