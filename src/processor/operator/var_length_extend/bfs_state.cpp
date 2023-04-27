#include "processor/operator/var_length_extend/bfs_state.h"

namespace kuzu {
namespace processor {

BFSMorsel::BFSMorsel(common::offset_t maxOffset_, uint8_t upperBound_) {
    maxOffset = maxOffset_;
    upperBound = upperBound_;
    currentFrontier = std::make_unique<Frontier>();
    nextFrontier = std::make_unique<Frontier>();
    visitedNodesBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
    visitedNodes = visitedNodesBuffer.get();
    distanceBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
    distance = distanceBuffer.get();
    resetState();
}

void BFSMorsel::resetState() {
    currentLevel = 0;
    nextNodeIdxToExtend = 0;
    currentFrontier->resetState();
    nextFrontier->resetState();
    numVisitedNodes = 0;
    std::fill(visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED);
}

bool BFSMorsel::isComplete() {
    if (currentFrontier->size() == 0) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == maxOffset) { // all destinations have been reached.
        return true;
    }
    return false;
}

void BFSMorsel::markSrc(common::offset_t offset) {
    visitedNodes[offset] = VISITED;
    distance[offset] = 0;
    numVisitedNodes++;
    currentFrontier->nodeOffsets.push_back(offset);
    srcOffset = offset;
}

void BFSMorsel::unmarkSrc() {
    visitedNodes[srcOffset] = NOT_VISITED;
    numVisitedNodes--;
}

void BFSMorsel::markVisited(common::offset_t offset) {
    assert(visitedNodes[offset] == NOT_VISITED);
    visitedNodes[offset] = VISITED;
    distance[offset] = currentLevel + 1;
    numVisitedNodes++;
    nextFrontier->nodeOffsets.push_back(offset);
}

common::offset_t BFSMorsel::getNextNodeOffset() {
    if (nextNodeIdxToExtend == currentFrontier->size()) {
        return common::INVALID_NODE_OFFSET;
    }
    return currentFrontier->nodeOffsets[nextNodeIdxToExtend++];
}

void BFSMorsel::moveNextLevelAsCurrentLevel() {
    currentFrontier = std::move(nextFrontier);
    currentLevel++;
    nextNodeIdxToExtend = 0;
    nextFrontier = std::make_unique<Frontier>();
    if (currentLevel == upperBound) { // No need to sort if we are not extending further.
        std::sort(currentFrontier->nodeOffsets.begin(), currentFrontier->nodeOffsets.end());
    }
}

} // namespace processor
} // namespace kuzu
