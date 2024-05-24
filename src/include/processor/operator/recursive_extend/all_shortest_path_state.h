#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
class AllShortestPathState : public BaseBFSState {
public:
    AllShortestPathState(uint8_t upperBound, TargetDstNodes* targetDstNodes)
        : BaseBFSState{upperBound, targetDstNodes}, minDistance{0}, numVisitedDstNodes{0} {}

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() ||
               isAllDstReachedWithMinDistance();
    }

    inline void resetState() final {
        BaseBFSState::resetState();
        minDistance = 0;
        numVisitedDstNodes = 0;
        visitedNodeToDistance.clear();
    }

    inline void markSrc(common::nodeID_t nodeID) override {
        visitedNodeToDistance.insert({nodeID, -1});
        if (targetDstNodes->contains(nodeID)) {
            numVisitedDstNodes++;
        }
        currentFrontier->addNodeWithMultiplicity(nodeID, 1);
    }

    void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        if (!visitedNodeToDistance.contains(nbrNodeID)) {
            visitedNodeToDistance.insert({nbrNodeID, (int64_t)currentLevel});
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

private:
    inline bool isAllDstReachedWithMinDistance() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes() && currentLevel > minDistance;
    }

private:
    uint32_t minDistance; // Min distance to add dst nodes that have been reached.
    uint64_t numVisitedDstNodes;
    common::node_id_map_t<int64_t> visitedNodeToDistance;
};

} // namespace processor
} // namespace kuzu
