#include "processor/operator/recursive_extend/bfs_state.h"

namespace kuzu {
namespace processor {

common::offset_t BaseBFSMorsel::getNextNodeOffset() {
    if (nextNodeIdxToExtend == currentFrontier->nodeOffsets.size()) {
        return common::INVALID_OFFSET;
    }
    return currentFrontier->nodeOffsets[nextNodeIdxToExtend++];
}

void BaseBFSMorsel::moveNextLevelAsCurrentLevel() {
    currentFrontier = nextFrontier;
    currentLevel++;
    nextNodeIdxToExtend = 0;
    if (currentLevel < upperBound) { // No need to sort if we are not extending further.
        addNextFrontier();
        std::sort(currentFrontier->nodeOffsets.begin(), currentFrontier->nodeOffsets.end());
    }
}

void BaseBFSMorsel::resetState() {
    currentLevel = 0;
    nextNodeIdxToExtend = 0;
    frontiers.clear();
    initStartFrontier();
    addNextFrontier();
    numTargetDstNodes = 0;
}

void ShortestPathBFSMorsel::markSrc(common::offset_t offset) {
    if (visitedNodes[offset] == NOT_VISITED_DST) {
        visitedNodes[offset] = VISITED_DST;
        numVisitedDstNodes++;
    }
    currentFrontier->nodeOffsets.push_back(offset);
}

void ShortestPathBFSMorsel::markVisited(common::offset_t boundOffset, common::offset_t nbrOffset) {
    if (visitedNodes[nbrOffset] == NOT_VISITED_DST) {
        visitedNodes[nbrOffset] = VISITED_DST;
        numVisitedDstNodes++;
        nextFrontier->addEdge(boundOffset, nbrOffset);
    } else if (visitedNodes[nbrOffset] == NOT_VISITED) {
        visitedNodes[nbrOffset] = VISITED;
        nextFrontier->addEdge(boundOffset, nbrOffset);
    }
}

void ShortestPathBFSMorsel::resetVisitedState() {
    numVisitedDstNodes = 0;
    if (!isAllDstTarget()) {
        std::fill(visitedNodes, visitedNodes + getNumNodes(), (uint8_t)VisitedState::NOT_VISITED);
        for (auto offset : targetDstNodeOffsets) {
            visitedNodes[offset] = VisitedState::NOT_VISITED_DST;
        }
        numTargetDstNodes = targetDstNodeOffsets.size();
    } else {
        std::fill(
            visitedNodes, visitedNodes + getNumNodes(), (uint8_t)VisitedState::NOT_VISITED_DST);
        numTargetDstNodes = getNumNodes();
    }
}

} // namespace processor
} // namespace kuzu
