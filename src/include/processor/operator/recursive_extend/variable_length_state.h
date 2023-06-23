#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct VariableLengthMorsel : public BaseBFSMorsel {
    VariableLengthMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound} {}
    ~VariableLengthMorsel() override = default;

    inline void resetState() final { BaseBFSMorsel::resetState(); }

    inline void reset(uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPMorsel* ssspMorsel_) final {

    }

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

    inline uint64_t getNumVisitedDstNodes() final {}
};

} // namespace processor
} // namespace kuzu
