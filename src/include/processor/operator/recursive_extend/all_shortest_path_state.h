#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct AllShortestPathMorsel : public BaseBFSMorsel {
public:
    AllShortestPathMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, minDistance{0},
          numVisitedDstNodes{0}, prevDistMorselStartEndIdx{0u, 0u} {}

    ~AllShortestPathMorsel() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() ||
               isAllDstReachedWithMinDistance();
    }

    inline void resetState() final {
        BaseBFSMorsel::resetState();
        minDistance = 0;
        numVisitedDstNodes = 0;
        visitedNodeToDistance.clear();
    }

    inline void markSrc(common::nodeID_t nodeID) override {
        visitedNodeToDistance.insert({nodeID, 0});
        if (targetDstNodes->contains(nodeID)) {
            numVisitedDstNodes++;
        }
        currentFrontier->addNodeWithMultiplicity(nodeID, 1);
    }

    void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        if (!visitedNodeToDistance.contains(nbrNodeID)) {
            visitedNodeToDistance.insert({nbrNodeID, currentLevel});
            if (targetDstNodes->contains(nbrNodeID)) {
                minDistance = currentLevel;
                numVisitedDstNodes++;
            }
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
            }
        } else if (currentLevel <= visitedNodeToDistance.at(nbrNodeID)) {
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
            }
        }
    }

    /// Following functions added for nTkS scheduler
    inline uint64_t getNumVisitedDstNodes() { return numVisitedDstNodes; }

    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
        numVisitedDstNodes = 0u;
    }

    // For Shortest Path, multiplicity is always 0
    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override {
        return bfsSharedState->nodeIDToMultiplicity[offset];
    }

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(
        common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) override;

    inline bool hasMoreToWrite() override {
        return prevDistMorselStartEndIdx.first < prevDistMorselStartEndIdx.second;
    }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        return {prevDistMorselStartEndIdx.first,
            prevDistMorselStartEndIdx.second - prevDistMorselStartEndIdx.first};
    }

    int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) override;

private:
    inline bool isAllDstReachedWithMinDistance() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes() && currentLevel > minDistance;
    }

private:
    uint32_t minDistance; // Min distance to add dst nodes that have been reached.
    uint64_t numVisitedDstNodes;
    frontier::node_id_map_t<uint32_t> visitedNodeToDistance;

    /// NEW ADDITION for [Single Label, Track None] to track start, end index of morsel.
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    std::pair<uint64_t, uint64_t> prevDistMorselStartEndIdx;
};

} // namespace processor
} // namespace kuzu
