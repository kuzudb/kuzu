#pragma once

#include <utility>

#include "common/query_rel_type.h"
#include "frontier.h"
#include "processor/operator/mask.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

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
 * The Lifecycle of an BFSSharedState in nTkS scheduler are the 3 following states. Depending on
 * which state an BFSSharedState is in, the thread will held that state :-
 *
 * 1) EXTEND_IN_PROGRESS: thread helps in level extension, try to get a morsel if available
 *
 * 2) PATH_LENGTH_WRITE_IN_PROGRESS: thread helps in writing path length to vector
 *
 * 3) MORSEL_COMPLETE: morsel is complete, try to launch another BFSSharedState (if available),
 *                     else find an existing one
 */
enum SSSPLocalState {
    EXTEND_IN_PROGRESS,
    PATH_LENGTH_WRITE_IN_PROGRESS,
    MORSEL_COMPLETE,

    // NOTE: This is an intermediate state returned ONLY when no work could be provided to a thread.
    // It will not be assigned to the local SSSPLocalState inside a BFSSharedState.
    NO_WORK_TO_SHARE
};

/**
 * Global states of MorselDispatcher used primarily for nTkS scheduler :-
 *
 * 1) IN_PROGRESS: Globally (across all threads active), there is still work available.
 * BFSSharedStates are available for threads to launch.
 *
 * 2) IN_PROGRESS_ALL_SRC_SCANNED: All BFSSharedState available to be launched have been launched.
 * It indicates that the inputFTable has no more tuples to be scanned. The threads have to search
 * for an active BFSSharedState (either in EXTEND_IN_PROGRESS / PATH_LENGTH_WRITE_IN_PROGRESS) to
 * execute.
 *
 * 3) COMPLETE: All BFSSharedStates have been completed, there is no more work either in BFS
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

/// Each element of nodeIDMultiplicityToLevel vector for Variable Length BFSSharedState
struct multiplicityAndLevel {
    std::atomic<uint64_t> multiplicity;
    uint8_t bfsLevel;
    std::shared_ptr<multiplicityAndLevel> next;

    multiplicityAndLevel(
        uint64_t multiplicity_, uint8_t bfsLevel_, std::shared_ptr<multiplicityAndLevel> next_) {
        multiplicity.store(multiplicity_, std::memory_order_relaxed);
        bfsLevel = bfsLevel_;
        next = std::move(next_);
    }

    virtual ~multiplicityAndLevel() = default;
};

/**
 * An BFSSharedState is a unit of work for the nTkS scheduling policy. It is *ONLY* used for the
 * particular case of SINGLE_LABEL | NO_PATH_TRACKING. A BFSSharedState is *NOT*
 * exclusive to a thread and any thread can pick up BFS extension or Writing Path Length.
 * A shared_ptr is maintained by the BaseBFSMorsel and a morsel of work is fetched using this ptr.
 */
struct BFSSharedState {
public:
    BFSSharedState(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{EXTEND_IN_PROGRESS}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsBFSActive{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          pathLengthThreadWriters{std::unordered_set<std::thread::id>()} {}

    inline bool isComplete() const { return ssspLocalState == MORSEL_COMPLETE; }

    /** This is a barrier condition to prevent Threads from unintentionally marking a BFSSharedState
     * as MORSEL_COMPLETE. There are 2 conditions being checked:
     *
     * i) If the thread trying to complete the BFSSharedState was involved in its path length
     *    writing stage. If its ID is not in the set pathLengthThreadWriters, it CANNOT mark the
     *    shared state as MORSEL_COMPLETE. This is because there can be multiple threads trying to
     *    fetch a path length morsel leading to duplicate decrements of numActiveBFSSharedState
     *    global count.
     *
     * ii) If numThreadsBFSActive is 0. If not, it means some thread which did BFS extension is
     *     still waiting to hold the lock for the BFSSharedState (waiting to enter the
     *     finishBFSMorsel function). Until it has decremented the count, this shared state can't
     *     be reused to launch another BFSSharedState.
     *     Main reason for this is to prevent the ABA problem, where if we ignore
     *     numThreadsBFSActive and mark it as MORSEL_COMPLETE and the morsel dispatcher decides to
     *     reuse it and resets the state back to EXTEND_IN_PROGRESS. Now the previous thread waiting
     *     would enter finishBFSMorsel and merge its local results to the new BFSSharedState.
     *
     *  Overall the purpose of this function is to prevent ABA problem and duplicate decrements of
     *  global active BFSSharedState count.
     */
    inline bool canThreadCompleteSharedState(std::thread::id threadID) const {
        if (!pathLengthThreadWriters.contains(threadID)) {
            return false;
        }
        if (numThreadsBFSActive > 0) {
            return false;
        }
        return true;
    }

    void reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType);

    SSSPLocalState getBFSMorsel(BaseBFSMorsel* bfsMorsel);

    bool hasWork() const;

    bool finishBFSMorsel(BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType);

    // If BFS has completed.
    bool isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType);
    // Mark src as visited.
    void markSrc(bool isSrcDestination, common::QueryRelType queryRelType);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstPathLengthMorsel();

public:
    std::mutex mutex;
    SSSPLocalState ssspLocalState;
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
    uint32_t numThreadsBFSActive;
    uint64_t nextDstScanStartIdx;
    uint64_t inputFTableTupleIdx;
    // To track which threads are writing path lengths to their ValueVectors
    std::unordered_set<std::thread::id> pathLengthThreadWriters;

    // FOR ALL_SHORTEST_PATH only
    uint8_t minDistance;
    std::vector<uint64_t> nodeIDToMultiplicity;

    // For VARIABLE_LENGTH only
    std::vector<std::shared_ptr<multiplicityAndLevel>> nodeIDMultiplicityToLevel;
};

struct BaseBFSMorsel {
public:
    BaseBFSMorsel(TargetDstNodes* targetDstNodes, uint8_t upperBound, uint8_t lowerBound)
        : targetDstNodes{targetDstNodes}, upperBound{upperBound}, lowerBound{lowerBound},
          currentLevel{0u}, bfsSharedState{nullptr} {}

    virtual ~BaseBFSMorsel() = default;

    virtual bool getRecursiveJoinType() = 0;

    inline bool hasBFSSharedState() const { return bfsSharedState != nullptr; }

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

    inline SSSPLocalState getBFSMorsel() { return bfsSharedState->getBFSMorsel(this); }

    inline bool finishBFSMorsel(common::QueryRelType queryRelType) {
        return bfsSharedState->finishBFSMorsel(this, queryRelType);
    }

    /// This is used for nTkSCAS scheduler case (no tracking of path + single label case)
    virtual void reset(
        uint64_t startScanIdx, uint64_t endScanIdx, BFSSharedState* bfsSharedState) = 0;

    virtual uint64_t getBoundNodeMultiplicity(common::offset_t nodeOffset) = 0;

#if defined(__GNUC__) || defined(__GNUG__)
    virtual void addToLocalNextBFSLevel(
        common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) = 0;
#endif

    virtual common::offset_t getNextNodeOffset() = 0;

    virtual bool hasMoreToWrite() = 0;

    virtual std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() = 0;

    virtual int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) = 0;

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
    BFSSharedState* bfsSharedState;
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
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
        numVisitedDstNodes = 0u;
    }

    // For Shortest Path, multiplicity is always 0
    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override { return 0u; }

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

#if defined(__GNUC__) || defined(__GNUG__)
    void addToLocalNextBFSLevel(
        common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) override;
#endif

    // For Shortest Path, this function will always return false, because there is no nodeID
    // multiplicity. Each distance morsel will exactly fit into ValueVector size perfectly.
    inline bool hasMoreToWrite() override { return false; }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        throw common::NotImplementedException("");
    }

    int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) override;

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
