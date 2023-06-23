#pragma once

#include "frontier.h"
#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
    VISITED_NEW = 4,
    VISITED_DST_NEW = 5,
};

// SSSPMorsel States
enum SSSPLocalState {
    MORSEL_EXTEND_IN_PROGRESS,
    MORSEL_DISTANCE_WRITE_IN_PROGRESS,
    MORSEL_COMPLETE
};

// Global States for MorselDispatcher
enum GlobalSSSPState { IN_PROGRESS, IN_PROGRESS_ALL_SRC_SCANNED, COMPLETE };

// Target dst nodes are populated from semi mask and is expected to have small size.
// TargetDstNodeOffsets is empty if no semi mask available. Note that at the end of BFS, we may
// not visit all target dst nodes because they may simply not connect to src.
class TargetDstNodes {
public:
    TargetDstNodes(uint64_t numNodes, frontier::node_id_set_t nodeIDs)
        : numNodes{numNodes}, nodeIDs{std::move(nodeIDs)} {}

    inline void setTableIDFilter(std::unordered_set<common::table_id_t> filter) {
        tableIDFilter = std::move(filter);
    }

    inline bool contains(common::nodeID_t nodeID) {
        if (nodeIDs.empty()) {           // no semi mask available
            if (tableIDFilter.empty()) { // no dst table ID filter available
                return true;
            }
            return tableIDFilter.contains(nodeID.tableID);
        }
        return nodeIDs.contains(nodeID);
    }

    inline uint64_t getNumNodes() const { return numNodes; }

    inline frontier::node_id_set_t getNodeIDs() const { return nodeIDs; }

private:
    uint64_t numNodes;
    frontier::node_id_set_t nodeIDs;
    std::unordered_set<common::table_id_t> tableIDFilter;
};

struct BaseBFSMorsel;

struct SSSPMorsel {
public:
    SSSPMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{MORSEL_EXTEND_IN_PROGRESS}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          distance{std::vector<uint16_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          lvlStartTimeInMillis{0u}, startTimeInMillis{0u}, distWriteStartTimeInMillis{0u} {}

    void reset(TargetDstNodes* targetDstNodes);

    SSSPLocalState getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    bool finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    // If BFS has completed.
    bool isComplete(uint64_t numDstNodesToVisit);
    // Mark src as visited.
    void markSrc(bool isSrcDestination);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstDistanceMorsel();

public:
    std::mutex mutex;
    SSSPLocalState ssspLocalState;
    // Level state
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;
    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<uint16_t> distance;
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
    uint64_t lvlStartTimeInMillis;
    uint64_t startTimeInMillis;
    uint64_t distWriteStartTimeInMillis;
};

struct BaseBFSMorsel {
public:
    BaseBFSMorsel(TargetDstNodes* targetDstNodes, uint8_t upperBound, uint8_t lowerBound)
        : targetDstNodes{targetDstNodes}, upperBound{upperBound}, lowerBound{lowerBound},
          currentLevel{0u}, threadCheckSSSPState{true}, ssspMorsel{nullptr} {}

    virtual ~BaseBFSMorsel() = default;

    // Get next node offset to extend from current level.
    common::nodeID_t getNextNodeID() {
        if (nextNodeIdxToExtend == currentFrontier->nodeIDs.size()) {
            return common::nodeID_t{common::INVALID_OFFSET, common::INVALID_TABLE_ID};
        }
        return currentFrontier->nodeIDs[nextNodeIdxToExtend++];
    }

    virtual void resetState() {
        currentLevel = 0;
        nextNodeIdxToExtend = 0;
        frontiers.clear();
        initStartFrontier();
        addNextFrontier();
    }

    virtual void reset(uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPMorsel* ssspMorsel_) = 0;

    virtual bool isComplete() = 0;

    virtual void markSrc(common::nodeID_t nodeID) = 0;
    virtual void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) = 0;
    virtual uint64_t getNumVisitedDstNodes() = 0;
    inline uint64_t getMultiplicity(common::nodeID_t nodeID) const {
        return currentFrontier->getMultiplicity(nodeID);
    }

    inline void finalizeCurrentLevel() { moveNextLevelAsCurrentLevel(); }
    inline size_t getNumFrontiers() const { return frontiers.size(); }
    inline Frontier* getFrontier(common::vector_idx_t idx) const { return frontiers[idx].get(); }

protected:
    inline bool isCurrentFrontierEmpty() const { return currentFrontier->nodeIDs.empty(); }
    inline bool isUpperBoundReached() const { return currentLevel == upperBound; }
    inline void initStartFrontier() {
        assert(frontiers.empty());
        frontiers.push_back(std::make_unique<Frontier>());
        currentFrontier = frontiers[frontiers.size() - 1].get();
    }
    inline void addNextFrontier() {
        frontiers.push_back(std::make_unique<Frontier>());
        nextFrontier = frontiers[frontiers.size() - 1].get();
    }
    void moveNextLevelAsCurrentLevel() {
        currentFrontier = nextFrontier;
        currentLevel++;
        nextNodeIdxToExtend = 0;
        if (currentLevel < upperBound) { // No need to sort if we are not extending further.
            addNextFrontier();
            std::sort(currentFrontier->nodeIDs.begin(), currentFrontier->nodeIDs.end());
        }
    }

protected:
    // Static information
    uint8_t upperBound;
    uint8_t lowerBound;
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    Frontier* currentFrontier;
    Frontier* nextFrontier;
    std::vector<std::unique_ptr<Frontier>> frontiers;
    // Target information.
public:
    TargetDstNodes* targetDstNodes;
    bool threadCheckSSSPState;
    SSSPMorsel* ssspMorsel;
};

template<bool TRACK_PATH>
class ShortestPathMorsel : public BaseBFSMorsel {
public:
    ShortestPathMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, numVisitedDstNodes{0u},
          startScanIdx{0u}, endScanIdx{0u} {}

    ~ShortestPathMorsel() override = default;

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

    inline uint64_t getNumVisitedDstNodes() final { return numVisitedDstNodes; }

    /// This is used for nTkSCAS scheduler case (no tracking of path + single label case)
    inline void reset(uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPMorsel* ssspMorsel_) final {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        ssspMorsel = ssspMorsel_;
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
    /// These will be used for:
    /// 1) Single Label, Track Path
    /// 2) Multi Label,  Track None
    /// 3) Multi Label,  Track Path
    uint64_t numVisitedDstNodes;
    frontier::node_id_set_t visited;

    /// These will be used for:
    /// 1) Single Label, Track None
    uint64_t startScanIdx;
    uint64_t endScanIdx;
};

} // namespace processor
} // namespace kuzu
