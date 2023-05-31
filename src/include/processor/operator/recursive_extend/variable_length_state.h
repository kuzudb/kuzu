#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct VariableLengthState : public BaseBFSState {
    VariableLengthState(uint8_t upperBound, TargetDstNodes* targetDstNodes)
        : BaseBFSState{upperBound, targetDstNodes} {}
    ~VariableLengthState() override = default;

    inline void resetState() final { BaseBFSState::resetState(); }
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
};

} // namespace processor
} // namespace kuzu
