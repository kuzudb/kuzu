#pragma once

#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

struct Frontier {
    std::vector<common::offset_t> nodeOffsets;

    inline void resetState() { nodeOffsets.clear(); }
    inline uint64_t size() const { return nodeOffsets.size(); }
};

struct BFSMorsel {
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    std::unique_ptr<Frontier> currentFrontier;
    std::unique_ptr<Frontier> nextFrontier;

    // Visited state
    uint64_t numDstNodes;
    uint64_t numVisitedDstNodes;
    uint8_t* visitedNodes;
    uint8_t* distance;

    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint8_t upperBound;

    BFSMorsel(common::offset_t maxOffset_, uint8_t upperBound_);

    // Reset state for a new src node.
    void resetState(NodeOffsetSemiMask* semiMask);
    // If BFS has completed.
    bool isComplete();
    // Mark src as visited.
    void markSrc(common::offset_t offset);
    // UnMark src as NOT visited to avoid outputting src which has length 0 path and thus should be
    // omitted.
    void unmarkSrc();
    // Mark node as a dst visited.
    void markVisitedDst(common::offset_t offset);
    // Mark node as visited.
    void markVisited(common::offset_t offset);

    // Get next node offset to extend from current level.
    common::offset_t getNextNodeOffset();
    void moveNextLevelAsCurrentLevel();

private:
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
    std::unique_ptr<uint8_t[]> distanceBuffer;
};

} // namespace processor
} // namespace kuzu
