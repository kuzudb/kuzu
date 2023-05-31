#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct ShortestPathState : public BaseBFSState {
    // Visited state
    uint64_t numVisitedDstNodes;
    frontier::node_id_set_t visited;

    ShortestPathState(uint8_t upperBound, TargetDstNodes* targetDstNodes)
        : BaseBFSState{upperBound, targetDstNodes}, numVisitedDstNodes{0} {}
    ~ShortestPathState() override = default;

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
    }
    inline void resetState() final {
        BaseBFSState::resetState();
        resetVisitedState();
    }

    inline void markSrc(common::nodeID_t nodeID) final {
        visited.insert(nodeID);
        if (targetDstNodes->contains(nodeID)) {
            numVisitedDstNodes++;
        }
        currentFrontier->addNode(nodeID);
    }

    inline void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::nodeID_t relID, uint64_t multiplicity) final {
        if (visited.contains(nbrNodeID)) {
            return;
        }
        visited.insert(nbrNodeID);
        if (targetDstNodes->contains(nbrNodeID)) {
            numVisitedDstNodes++;
        }
        if constexpr (TRACK_PATH) {
            nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
        } else {
            nextFrontier->addNode(nbrNodeID);
        }
    }

    inline bool isAllDstReached() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes();
    }
    inline void resetVisitedState() {
        numVisitedDstNodes = 0;
        visited.clear();
    }
};

} // namespace processor
} // namespace kuzu
