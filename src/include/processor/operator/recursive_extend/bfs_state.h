#pragma once

#include "frontier.h"
#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

/**
 * States used for nTkS scheduler to mark different status of node offsets.
 * VISITED_NEW and VISITED_DST_NEW are the states when a node is initially visited and
 * when we scan to prepare the bfsLevelNodes vector for next level extension,
 * VISITED_NEW -> VISITED | VISITED_DST_NEW -> VISITED_DST
 */
enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
    VISITED_NEW = 4,
    VISITED_DST_NEW = 5,
};

/**
 * The Lifecycle of an SSSPSharedState in nTkS scheduler are the 3 following states. Depending on
 * which state an SSSPSharedState is in, the thread will held that state :-
 *
 * 1) EXTEND_IN_PROGRESS: thread helps in level extension, try to get a morsel if available
 *
 * 2) PATH_LENGTH_WRITE_IN_PROGRESS: thread helps in writing path length to vector
 *
 * 3) MORSEL_COMPLETE: morsel is complete, try to launch another SSSPSharedState (if available),
 *                     else find an existing one
 */
enum SSSPLocalState {
    EXTEND_IN_PROGRESS,
    PATH_LENGTH_WRITE_IN_PROGRESS,
    MORSEL_COMPLETE,

    // NOTE: This is an intermediate state returned ONLY when no work could be provided to a thread.
    // It will not be assigned to the local SSSPLocalState inside a SSSPSharedState.
    NO_WORK_TO_SHARE
};

/**
 * Global states of MorselDispatcher used primarily for nTkS scheduler :-
 *
 * 1) IN_PROGRESS: Globally (across all threads active), there is still work available.
 * SSSPSharedStates are available for threads to launch.
 *
 * 2) IN_PROGRESS_ALL_SRC_SCANNED: All SSSPSharedState available to be launched have been launched.
 * It indicates that the inputFTable has no more tuples to be scanned. The threads have to search
 * for an active SSSPSharedState (either in EXTEND_IN_PROGRESS / PATH_LENGTH_WRITE_IN_PROGRESS) to
 * execute.
 *
 * 3) COMPLETE: All SSSPSharedStates have been completed, there is no more work either in BFS
 * extension or path length writing to vector. Threads can exit completely (return false and return
 * to parent operator).
 */
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

/**
 * An SSSPSharedState is a unit of work for the nTkS scheduling policy. It is *ONLY* used for the
 * particular case of SHORTEST_PATH | SINGLE_LABEL | NO_PATH_TRACKING. An SSSPSharedState is *NOT*
 * exclusive to a thread and any thread can pick up BFS extension or Writing Path Length.
 * A shared_ptr is maintained by the BaseBFSMorsel and a morsel of work is fetched using this ptr.
 */
struct SSSPSharedState {
public:
    SSSPSharedState(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{EXTEND_IN_PROGRESS}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u} {}

    void reset(TargetDstNodes* targetDstNodes);

    bool getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel, SSSPLocalState& state);

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
};

struct BaseBFSMorsel {
public:
    BaseBFSMorsel(TargetDstNodes* targetDstNodes, uint8_t upperBound, uint8_t lowerBound)
        : targetDstNodes{targetDstNodes}, upperBound{upperBound}, lowerBound{lowerBound},
          currentLevel{0u}, ssspSharedState{nullptr} {}

    virtual ~BaseBFSMorsel() = default;

    virtual bool getRecursiveJoinType() = 0;

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

    virtual bool isComplete() = 0;

    virtual void markSrc(common::nodeID_t nodeID) = 0;
    virtual void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) = 0;
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
    SSSPSharedState* ssspSharedState;
};

template<bool TRACK_PATH>
class ShortestPathMorsel : public BaseBFSMorsel {
public:
    ShortestPathMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, numVisitedDstNodes{0u},
          startScanIdx{0u}, endScanIdx{0u} {}

    ~ShortestPathMorsel() override = default;

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
    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPSharedState* ssspSharedState_) {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        ssspSharedState = ssspSharedState_;
        numVisitedDstNodes = 0u;
    }

    inline common::offset_t getNextNodeOffset() {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return ssspSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(common::ValueVector* tmpDstNodeIDVector);

private:
    inline bool isAllDstReached() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes();
    }

public:
    uint64_t numVisitedDstNodes;
    frontier::node_id_set_t visited;

    /// These will be used for [Single Label, Track None] to track starting, ending index of morsel.
    uint64_t startScanIdx;
    uint64_t endScanIdx;
};

} // namespace processor
} // namespace kuzu
