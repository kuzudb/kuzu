#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct VariableLengthMorsel : public BaseBFSMorsel {
    VariableLengthMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound} {}
    ~VariableLengthMorsel() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline void resetState() final { BaseBFSMorsel::resetState(); }

    inline bool isComplete() final { return isCurrentFrontierEmpty() || isUpperBoundReached(); }

    inline void markSrc(common::nodeID_t nodeID) final {
        currentFrontier->addNodeWithMultiplicity(nodeID, 1 /* multiplicity */);
    }

    inline void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        if constexpr (TRACK_PATH) {
            nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
        } else {
            nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
        }
    }

    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
    }

    inline uint64_t getBoundNodeMultiplicity(common::offset_t nodeOffset) override {
        auto topEntry = bfsSharedState->nodeIDMultiplicityToLevel[nodeOffset];
        while (topEntry && topEntry->bfsLevel != bfsSharedState->currentLevel) {
            topEntry = topEntry->next;
        }
        return topEntry->multiplicity.load(std::memory_order_relaxed);
    }

#if defined(__GNUC__) || defined(__GNUG__)
    void addToLocalNextBFSLevel(
        common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) override;
#endif

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

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
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    std::pair<uint64_t, uint64_t> prevDistMorselStartEndIdx;
};

} // namespace processor
} // namespace kuzu
