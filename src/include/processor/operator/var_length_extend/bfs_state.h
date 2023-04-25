#pragma once

#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED = 0,
    VISITED = 1,
};

struct BFSLevel {
    std::vector<common::offset_t> nodeOffsets;

    inline void resetState() { nodeOffsets.clear(); }
    inline uint64_t size() const { return nodeOffsets.size(); }
};

struct SSPMorsel {
    // Level state
    uint8_t currentLevelNumber;
    uint64_t currentLevelNodeIdx; // controls scan of current BFS level
    std::unique_ptr<BFSLevel> currentBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;

    // Visited state
    uint64_t numVisitedNodes;
    uint8_t* visitedNodes;
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
    uint8_t* distance;
    std::unique_ptr<uint8_t[]> distanceBuffer;

    // Start offset of src node. We need to exclude start node from result.
    common::offset_t startOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint8_t upperBound;

    SSPMorsel(common::offset_t maxOffset_, uint8_t upperBound_);

    // Reset state for a new src node.
    void resetState();
    // If BFS has completed.
    bool isComplete();
    // Mark src as visited.
    void markSrc(common::offset_t offset);
    // UnMark src as NOT visited to avoid output src.
    void unMarkSrc();
    // Mark node as visited.
    void markVisited(common::offset_t offset);

    // Get next node offset to extend from current level.
    common::offset_t getNextNodeOffset();
    void moveNextLevelAsCurrentLevel();
};

} // namespace processor
} // namespace kuzu
