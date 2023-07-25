/*
#include "processor/operator/recursive_extend/all_shortest_path_state_temp.h"

namespace kuzu {
namespace processor {

// CAN BE SHARED, MOSTLY SAME EXCEPT FOR THE nodeIDToMultiplicity and minDistance members.
void AllShortestSSSPMorsel::reset(kuzu::processor::TargetDstNodes* targetDstNodes) {
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
    minDistance = 0u;
    std::fill(nodeIDToMultiplicity.begin(), nodeIDToMultiplicity.end(), 0u);
}

// NEEDS TO BE CHANGED
// From ShortestPathMorsel, it will be a AllShortestPathMorsel. Else most of the logic is same,
// can be shared.
SSSPLocalState AllShortestSSSPMorsel::getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    mutex.lock();
    if (ssspLocalState == MORSEL_COMPLETE || ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS) {
        bfsMorsel->threadCheckSSSPState = true;
        mutex.unlock();
        return ssspLocalState;
    }
    if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        numThreadsActiveOnMorsel++;
        auto bfsMorselSize = std::min(
            common::DEFAULT_VECTOR_CAPACITY, bfsLevelNodeOffsets.size() - nextScanStartIdx);
        auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
        /// TODO: To be changed to AllShortestPathMorselTemp cast
        auto allShortestPathMorselTemp =
            (reinterpret_cast<AllShortestPathMorselTemp<false>*>(bfsMorsel.get()));
        allShortestPathMorselTemp->reset(nextScanStartIdx, morselScanEndIdx, this);
        nextScanStartIdx += bfsMorselSize;
        mutex.unlock();
        return ssspLocalState;
    }
    bfsMorsel->threadCheckSSSPState = true;
    mutex.unlock();
    return ssspLocalState;
}

// SAME LOGIC AS SSSPMorsel, code can be shared
bool AllShortestSSSPMorsel::hasWork() const {
    if (ssspLocalState == EXTEND_IN_PROGRESS && nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        return true;
    } else if (ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS &&
               nextDstScanStartIdx < pathLength.size()) {
        return true;
    } else {
        return false;
    }
}

bool AllShortestSSSPMorsel::finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {}

// CANNOT BE SHARED, TERMINATION CONDITIONS ARE DIFFERENT
bool AllShortestSSSPMorsel::isComplete(uint64_t numDstNodesToVisit) {
    if (bfsLevelNodeOffsets.empty()) {
        return true;
    }
    if (currentLevel == upperBound) {
        return true;
    }
    if (numVisitedNodes > numDstNodesToVisit && currentLevel > minDistance) {
        return true;
    }
    return false;
}

// CANNOT BE SHARED, there is nodeIDToMultiplicity getting updated.
void AllShortestSSSPMorsel::markSrc(bool isSrcDestination) {
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        pathLength[srcOffset] = 0u;
        nodeIDToMultiplicity[srcOffset] = 1;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    bfsLevelNodeOffsets.push_back(srcOffset);
}

// CAN BE SHARED, LOOKS TO BE THE SAME
void AllShortestSSSPMorsel::moveNextLevelAsCurrentLevel() {
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

std::pair<uint64_t, int64_t> AllShortestSSSPMorsel::getDstPathLengthMorsel() {
    mutex.lock();
    if (ssspLocalState != PATH_LENGTH_WRITE_IN_PROGRESS) {
        mutex.unlock();
        return {UINT64_MAX, INT64_MAX};
    } else if (nextDstScanStartIdx == visitedNodes.size()) {
        ssspLocalState = MORSEL_COMPLETE;
        mutex.unlock();
        return {0, -1};
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    mutex.unlock();
    return startScanIdxAndSize;
}

template<>
void AllShortestPathMorselTemp<false>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = allShortestSSSPMorsel->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &allShortestSSSPMorsel->visitedNodes[nodeID.offset], state, VISITED_DST_NEW)) {
                allShortestSSSPMorsel->pathLength[nodeID.offset] =
                    allShortestSSSPMorsel->currentLevel;
                numVisitedDstNodes++;
                /// TODO: MAYBE DO THIS IN THE mergeLocalBFSResults function, no need for CAS
                if (allShortestSSSPMorsel->minDistance < allShortestSSSPMorsel->currentLevel) {
                    __sync_bool_compare_and_swap(&allShortestSSSPMorsel->minDistance,
                        allShortestSSSPMorsel->minDistance, allShortestSSSPMorsel->currentLevel);
                }
            }
        } else if (state == NOT_VISITED) {
            __sync_bool_compare_and_swap(
                &allShortestSSSPMorsel->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
        auto currMultiplicity = allShortestSSSPMorsel->nodeIDToMultiplicity[nodeID.offset];
        __sync_bool_compare_and_swap(&allShortestSSSPMorsel->nodeIDToMultiplicity[nodeID.offset],
            currMultiplicity, currMultiplicity + 1);
    }
}

} // namespace processor
} // namespace kuzu
*/
