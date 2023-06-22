#pragma once

#include "bfs_state_temp.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct AllShortestPathMorsel : public BaseBFSMorsel {
public:
    AllShortestPathMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, minDistance{0}, numVisitedDstNodes{
                                                                                     0} {}

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

    inline void reset(uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPMorsel* ssspMorsel_) final {

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

    inline uint64_t getNumVisitedDstNodes() final { return numVisitedDstNodes; }

private:
    inline bool isAllDstReachedWithMinDistance() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes() && currentLevel > minDistance;
    }

private:
    uint32_t minDistance; // Min distance to add dst nodes that have been reached.
    uint64_t numVisitedDstNodes;
    frontier::node_id_map_t<uint32_t> visitedNodeToDistance;
};

} // namespace processor
} // namespace kuzu
