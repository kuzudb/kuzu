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
    currentFrontier = std::move(nextFrontier);
    currentLevel++;
    nextNodeIdxToExtend = 0;
    nextFrontier = createFrontier();
    if (currentLevel == upperBound) { // No need to sort if we are not extending further.
        std::sort(currentFrontier->nodeOffsets.begin(), currentFrontier->nodeOffsets.end());
    }
}

void BaseBFSMorsel::resetState() {
    currentLevel = 0;
    nextNodeIdxToExtend = 0;
    currentFrontier->resetState();
    nextFrontier->resetState();
    numTargetDstNodes = 0;
}

void ShortestPathBFSMorsel::markSrc(common::offset_t offset) {
    if (visitedNodes[offset] == NOT_VISITED_DST) {
        visitedNodes[offset] = VISITED_DST;
        numVisitedDstNodes++;
    }
    currentFrontier->nodeOffsets.push_back(offset);
}

void ShortestPathBFSMorsel::markVisited(common::offset_t offset, uint64_t multiplicity) {
    if (visitedNodes[offset] == NOT_VISITED_DST) {
        visitedNodes[offset] = VISITED_DST;
        dstNodeOffsets.push_back(offset);
        dstNodeOffset2PathLength.insert({offset, currentLevel + 1});
        numVisitedDstNodes++;
        nextFrontier->nodeOffsets.push_back(offset);
    } else if (visitedNodes[offset] == NOT_VISITED) {
        visitedNodes[offset] = VISITED;
        nextFrontier->nodeOffsets.push_back(offset);
    }
}

void ShortestPathBFSMorsel::resetVisitedState() {
    numVisitedDstNodes = 0;
    dstNodeOffsets.clear();
    dstNodeOffset2PathLength.clear();
    if (!isAllDstTarget()) {
        std::fill(visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED);
        for (auto offset : targetDstNodeOffsets) {
            visitedNodes[offset] = VisitedState::NOT_VISITED_DST;
        }
        numTargetDstNodes = targetDstNodeOffsets.size();
    } else {
        std::fill(
            visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED_DST);
        numTargetDstNodes = maxOffset + 1;
    }
}

void VariableLengthBFSMorsel::updateNumPathFromCurrentFrontier() {
    if (currentLevel < lowerBound) {
        return;
    }
    if (!isAllDstTarget() && numTargetDstNodes < currentFrontier->nodeOffsets.size()) {
        // Target is smaller than current frontier size. Loop through target instead of current
        // frontier.
        for (auto offset : targetDstNodeOffsets) {
            if (((FrontierWithMultiplicity&)*currentFrontier).contains(offset)) {
                updateNumPath(offset, currentFrontier->getMultiplicity(offset));
            }
        }
    } else {
        for (auto offset : currentFrontier->nodeOffsets) {
            updateNumPath(offset, currentFrontier->getMultiplicity(offset));
        }
    }
}

} // namespace processor
} // namespace kuzu
