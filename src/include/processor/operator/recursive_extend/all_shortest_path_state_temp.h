/*
#pragma once

#include "frontier.h"
#include "processor/operator/mask.h"
#include "processor/operator/recursive_extend/bfs_state.h"

namespace kuzu {
namespace processor {

*/
/**
 * An SSSPMorsel is a unit of work for the nTkS scheduling policy. It is *ONLY* used for the
 * particular case of SHORTEST_PATH | SINGLE_LABEL | NO_PATH_TRACKING. An SSSPMorsel is *NOT*
 * exclusive to a thread and any thread can pick up BFS extension or Writing Path Length.
 * A shared_ptr is maintained by the BaseBFSMorsel and a morsel of work is fetched using this ptr.
 *//*

struct AllShortestSSSPMorsel {
public:
    AllShortestSSSPMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{EXTEND_IN_PROGRESS}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          minDistance{0u}, nodeIDToMultiplicity{std::vector<uint64_t>(maxNodeOffset_ + 1, 0u)} {}

    void reset(TargetDstNodes* targetDstNodes);

    SSSPLocalState getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    bool hasWork() const;

    bool finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    // If BFS has completed.
    bool isComplete(uint64_t numDstNodesToVisit);
    // Mark src as visited.
    void markSrc(bool isSrcDestination);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstPathLengthMorsel();

public:
    std::mutex mutex;
    SSSPLocalState ssspLocalState;
    // Level state
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;
    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<uint8_t> pathLength;
    std::vector<common::offset_t> bfsLevelNodeOffsets;
    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t numThreadsActiveOnMorsel;
    uint64_t nextDstScanStartIdx;
    uint64_t inputFTableTupleIdx;

    /// NEWLY ADDED
    uint8_t minDistance;
    // Initially consider this vector, later other structures will be considered. Size of this
    // will be very high = 8 bytes * maxOffset | 1 million nodes => 7.6 MB
    std::vector<uint64_t> nodeIDToMultiplicity;
};

template<bool TRACK_PATH>
class AllShortestPathMorselTemp : public BaseBFSMorsel {
public:
    AllShortestPathMorselTemp(
        uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, numVisitedDstNodes{0u},
          startScanIdx{0u}, endScanIdx{0u}, allShortestSSSPMorsel{nullptr} {}

    ~AllShortestPathMorselTemp() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    /// This is used for 1T1S scheduler case (multi label, path tracking)
    inline void resetState() final {
        BaseBFSMorsel::resetState();
        numVisitedDstNodes = 0;
        visited.clear();
    }

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
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

    inline uint64_t getNumVisitedDstNodes() { return numVisitedDstNodes; }

    /// This is used for nTkSCAS scheduler case (no tracking of path + single label case)
    inline void reset(uint64_t startScanIdx_, uint64_t endScanIdx_,
        AllShortestSSSPMorsel* allShortestSSSPMorsel_) {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        allShortestSSSPMorsel = allShortestSSSPMorsel_;
        threadCheckSSSPState = false;
        numVisitedDstNodes = 0u;
    }

    inline common::offset_t getNextNodeOffset() {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return ssspMorsel->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(common::ValueVector* tmpDstNodeIDVector);

private:
    inline bool isAllDstReached() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes();
    }

public:
    AllShortestSSSPMorsel* allShortestSSSPMorsel;
    uint64_t numVisitedDstNodes;
    frontier::node_id_set_t visited;

    /// These will be used for [Single Label, Track None] to track starting, ending index of morsel.
    uint64_t startScanIdx;
    uint64_t endScanIdx;
};

} // namespace processor
} // namespace kuzu*/
