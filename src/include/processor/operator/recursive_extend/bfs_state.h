#pragma once

#include <algorithm>

#include "frontier.h"

namespace kuzu {
namespace processor {

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

private:
    uint64_t numNodes;
    frontier::node_id_set_t nodeIDs;
    std::unordered_set<common::table_id_t> tableIDFilter;
};

class BaseBFSState {
public:
    explicit BaseBFSState(uint8_t upperBound, TargetDstNodes* targetDstNodes)
        : upperBound{upperBound}, currentLevel{0}, nextNodeIdxToExtend{0}, targetDstNodes{
                                                                               targetDstNodes} {}
    virtual ~BaseBFSState() = default;

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
        KU_ASSERT(frontiers.empty());
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
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    Frontier* currentFrontier;
    Frontier* nextFrontier;
    std::vector<std::unique_ptr<Frontier>> frontiers;
    // Target information.
    TargetDstNodes* targetDstNodes;
};

} // namespace processor
} // namespace kuzu
