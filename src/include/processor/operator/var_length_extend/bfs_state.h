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
    // current/next level information
    uint8_t currentLevel;
    uint64_t currentLevelNodeIdx;
    std::unique_ptr<BFSLevel> currentBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;

    // visited nodes information
    uint64_t numVisitedNodes;
    uint8_t* visitedNodes;
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
    uint8_t* distance;
    std::unique_ptr<uint8_t[]> distanceBuffer;

    common::offset_t startOffset;

    SSPMorsel(common::offset_t maxOffset) {
        currentBFSLevel = std::make_unique<BFSLevel>();
        nextBFSLevel = std::make_unique<BFSLevel>();
        visitedNodesBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
        visitedNodes = visitedNodesBuffer.get();
        distanceBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
        distance = distanceBuffer.get();
    }

    void resetState(common::offset_t maxOffset) {
        currentLevel = 0;
        currentLevelNodeIdx = 0;
        currentBFSLevel->resetState();
        nextBFSLevel->resetState();
        numVisitedNodes = 0;
        std::fill(visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED);
    }

    bool isComplete(uint8_t upperBound, common::offset_t maxNodeOffset) {
        if (currentBFSLevel->size() == 0) {
            return true;
        }
        if (currentLevel == upperBound) {
            return true;
        }
        if (numVisitedNodes == maxNodeOffset) {
            return true;
        }
        return false;
    }

    void markSrc(common::offset_t offset) {
        visitedNodes[offset] = VISITED;
        distance[offset] = 0;
        numVisitedNodes++;
        currentBFSLevel->nodeOffsets.push_back(offset);
        startOffset = offset;
    }

    void markOnVisit(common::offset_t offset) {
        visitedNodes[offset] = VISITED;
        distance[offset] = currentLevel + 1;
        numVisitedNodes++;
        nextBFSLevel->nodeOffsets.push_back(offset);
    }

    common::offset_t getNextNodeOffset() {
        if (currentLevelNodeIdx == currentBFSLevel->size()) {
            return common::INVALID_NODE_OFFSET;
        }
        return currentBFSLevel->nodeOffsets[currentLevelNodeIdx++];
    }

    void moveNextLevelAsCurrentLevel(uint8_t upperBound) {
        currentBFSLevel = std::move(nextBFSLevel);
        currentLevel++;
        currentLevelNodeIdx = 0;
        std::sort(currentBFSLevel->nodeOffsets.begin(), currentBFSLevel->nodeOffsets.end());
        nextBFSLevel = std::make_unique<BFSLevel>();
    }
};

// BFS states shared by ScanBFSLevel, ScanRelTable, RecursiveJoin in the same thread.
struct SSPThreadLocalSharedState {
    std::unique_ptr<FactorizedTable> fTable;
    std::unique_ptr<SSPMorsel> sspMorsel;

    std::shared_ptr<common::ValueVector> tmpSrcNodeIDVector;
    std::shared_ptr<common::ValueVector> tmpDstNodeIDVector;

    SSPThreadLocalSharedState() {
        tmpSrcNodeIDVector = std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr);
        tmpSrcNodeIDVector->state = common::DataChunkState::getSingleValueDataChunkState();
        tmpDstNodeIDVector = std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr);
        tmpDstNodeIDVector->state = std::make_shared<common::DataChunkState>();
    }
};

} // namespace processor
} // namespace kuzu