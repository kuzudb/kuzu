#include "processor/operator/var_length_extend/bfs_state.h"

namespace kuzu {
namespace processor {

SSPMorsel::SSPMorsel(common::offset_t maxOffset_, uint8_t upperBound_) {
    maxOffset = maxOffset_;
    upperBound = upperBound_;
    currentBFSLevel = std::make_unique<BFSLevel>();
    nextBFSLevel = std::make_unique<BFSLevel>();
    visitedNodesBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
    visitedNodes = visitedNodesBuffer.get();
    distanceBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
    distance = distanceBuffer.get();
    resetState();
}

void SSPMorsel::resetState() {
    currentLevelNumber = 0;
    currentLevelNodeIdx = 0;
    currentBFSLevel->resetState();
    nextBFSLevel->resetState();
    numVisitedNodes = 0;
    std::fill(visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED);
}

bool SSPMorsel::isComplete() {
    if (currentBFSLevel->size() == 0) { // no more to extend.
        return true;
    }
    if (currentLevelNumber == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == maxOffset) { // all dst has been reached.
        return true;
    }
    return false;
}

void SSPMorsel::markSrc(common::offset_t offset) {
    visitedNodes[offset] = VISITED;
    distance[offset] = 0;
    numVisitedNodes++;
    currentBFSLevel->nodeOffsets.push_back(offset);
    startOffset = offset;
}

void SSPMorsel::unMarkSrc() {
    visitedNodes[startOffset] = NOT_VISITED;
    numVisitedNodes--;
}

void SSPMorsel::markVisited(common::offset_t offset) {
    assert(visitedNodes[offset] == NOT_VISITED);
    visitedNodes[offset] = VISITED;
    distance[offset] = currentLevelNumber + 1;
    numVisitedNodes++;
    nextBFSLevel->nodeOffsets.push_back(offset);
}

common::offset_t SSPMorsel::getNextNodeOffset() {
    if (currentLevelNodeIdx == currentBFSLevel->size()) {
        return common::INVALID_NODE_OFFSET;
    }
    return currentBFSLevel->nodeOffsets[currentLevelNodeIdx++];
}

void SSPMorsel::moveNextLevelAsCurrentLevel() {
    currentBFSLevel = std::move(nextBFSLevel);
    currentLevelNumber++;
    currentLevelNodeIdx = 0;
    nextBFSLevel = std::make_unique<BFSLevel>();
    if (currentLevelNumber == upperBound) { // No need to sort if we are not extending further.
        std::sort(currentBFSLevel->nodeOffsets.begin(), currentBFSLevel->nodeOffsets.end());
    }
}

} // namespace processor
} // namespace kuzu