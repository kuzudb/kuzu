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

} // namespace processor
} // namespace kuzu
