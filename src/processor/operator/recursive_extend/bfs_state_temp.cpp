#include "processor/operator/recursive_extend/bfs_state_temp.h"

namespace kuzu {
namespace processor {

void SSSPMorsel::reset(TargetDstNodes* targetDstNodes) {
    ssspLocalState = MORSEL_EXTEND_IN_PROGRESS;
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
    std::fill(distance.begin(), distance.end(), 0u);
    bfsLevelNodeOffsets.clear();
    srcOffset = 0u;
    numThreadsActiveOnMorsel = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    lvlStartTimeInMillis = 0u;
    startTimeInMillis = 0u;
    distWriteStartTimeInMillis = 0u;
}

/*
 * Returning the state here because if SSSPMorsel is complete / in distance writing stage then
 * depending on state we need to take next step. If MORSEL_COMPLETE then proceed to get a
 * new SSSPMorsel & if MORSEL_DISTANCE_WRITING_IN_PROGRESS then help in this task.
 */
SSSPLocalState SSSPMorsel::getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    mutex.lock();
    if (ssspLocalState == MORSEL_COMPLETE || ssspLocalState == MORSEL_DISTANCE_WRITE_IN_PROGRESS) {
        bfsMorsel->threadCheckSSSPState = true;
        mutex.unlock();
        return ssspLocalState;
    }
    if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        numThreadsActiveOnMorsel++;
        auto bfsMorselSize = std::min(
            common::DEFAULT_VECTOR_CAPACITY, bfsLevelNodeOffsets.size() - nextScanStartIdx);
        auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
        bfsMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
        nextScanStartIdx += bfsMorselSize;
        mutex.unlock();
        return ssspLocalState;
    }
    bfsMorsel->threadCheckSSSPState = true;
    mutex.unlock();
    return ssspLocalState;
}

bool SSSPMorsel::finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    mutex.lock();
    numThreadsActiveOnMorsel--;
    if (ssspLocalState != MORSEL_EXTEND_IN_PROGRESS) {
        mutex.unlock();
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    numVisitedNodes += bfsMorsel->getNumVisitedDstNodes();
    if (numThreadsActiveOnMorsel == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        auto size = bfsLevelNodeOffsets.size();
        moveNextLevelAsCurrentLevel();
        /*printf("%lu total nodes in level: %d finished in %lu ms\n", size, currentLevel - 1,
            millis - lvlStartTimeInMillis);*/
        lvlStartTimeInMillis = millis;
        if (isComplete(bfsMorsel->targetDstNodes->getNumNodes())) {
            ssspLocalState = MORSEL_DISTANCE_WRITE_IN_PROGRESS;
            mutex.unlock();
            return true;
        }
    } else if (isComplete(bfsMorsel->targetDstNodes->getNumNodes())) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        /*printf("%lu total nodes in level: %d | early finish in %lu ms\n",
            bfsLevelNodeOffsets.size(), currentLevel, millis - lvlStartTimeInMillis);*/
        ssspLocalState = MORSEL_DISTANCE_WRITE_IN_PROGRESS;
        mutex.unlock();
        return true;
    }
    mutex.unlock();
    return false;
}

bool SSSPMorsel::isComplete(uint64_t numDstNodesToVisit) {
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

void SSSPMorsel::markSrc(bool isSrcDestination) {
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        distance[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    bfsLevelNodeOffsets.push_back(srcOffset);
}

void SSSPMorsel::moveNextLevelAsCurrentLevel() {
    currentLevel++;
    nextScanStartIdx = 0u;
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
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
    auto duration3 = std::chrono::system_clock::now().time_since_epoch();
    auto millis3 = std::chrono::duration_cast<std::chrono::milliseconds>(duration3).count();
    /*printf("Time taken to prepare: %lu nodes is: %lu\n", bfsLevelNodeOffsets.size(),
        millis3 - millis2);*/
}

std::pair<uint64_t, int64_t> SSSPMorsel::getDstDistanceMorsel() {
    mutex.lock();
    if (ssspLocalState != MORSEL_DISTANCE_WRITE_IN_PROGRESS) {
        mutex.unlock();
        if (startTimeInMillis != 0) {
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            /*printf(
                "SSSP with src: %lu completed in: %lu ms\n", srcOffset, millis -
               startTimeInMillis);*/
        }
        return {UINT64_MAX, INT64_MAX};
    } else if (nextDstScanStartIdx == visitedNodes.size()) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        /*printf("SSSP with source: %lu took: %lu ms to write distances\n", srcOffset,
            millis - distWriteStartTimeInMillis);*/
        ssspLocalState = MORSEL_COMPLETE;
        mutex.unlock();
        return {0, -1};
    }
    if (nextDstScanStartIdx == 0u) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        distWriteStartTimeInMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    mutex.unlock();
    return startScanIdxAndSize;
}

template<>
void ShortestPathMorsel<false>::addToLocalNextBFSLevel(common::ValueVector* tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = ssspMorsel->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &ssspMorsel->visitedNodes[nodeID.offset], state, VISITED_DST_NEW)) {
                ssspMorsel->distance[nodeID.offset] = ssspMorsel->currentLevel + 1;
                numVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
            __sync_bool_compare_and_swap(
                &ssspMorsel->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
    }
}

} // namespace processor
} // namespace kuzu