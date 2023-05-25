#pragma once

#include "frontier.h"
#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

struct BaseBFSState {
    // Static information
    uint8_t upperBound;
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    Frontier* currentFrontier;
    Frontier* nextFrontier;
    std::vector<std::unique_ptr<Frontier>> frontiers;

    // Target information.
    // Target dst nodes are populated from semi mask and is expected to have small size.
    // TargetDstNodeOffsets is empty if no semi mask available. Note that at the end of BFS, we may
    // not visit all target dst nodes because they may simply not connect to src.
    uint64_t numTargetDstNodes;
    frontier::node_id_set_t targetDstNodeIDs;

    explicit BaseBFSState(uint8_t upperBound, const std::vector<NodeOffsetSemiMask*>& semiMasks,
        transaction::Transaction* trx)
        : upperBound{upperBound}, currentLevel{0}, nextNodeIdxToExtend{0}, numTargetDstNodes{0} {
        for (auto semiMask : semiMasks) {
            auto nodeTable = semiMask->getNodeTable();
            auto numNodes = nodeTable->getMaxNodeOffset(trx) + 1;
            if (semiMask->isEnabled()) {
                for (auto offset = 0u; offset < numNodes; ++offset) {
                    if (semiMask->isNodeMasked(offset)) {
                        targetDstNodeIDs.insert(common::nodeID_t{offset, nodeTable->getTableID()});
                        numTargetDstNodes++;
                    }
                }
            } else {
                numTargetDstNodes += numNodes;
            }
        }
    }
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

    inline void finalizeCurrentLevel() { moveNextLevelAsCurrentLevel(); }

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
};

} // namespace processor
} // namespace kuzu
