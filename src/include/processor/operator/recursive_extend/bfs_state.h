#pragma once

#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

struct Frontier {
    std::vector<common::offset_t> nodeOffsets;
    std::unordered_map<common::offset_t, std::vector<common::offset_t>> bwdEdges;

    Frontier() = default;
    inline virtual void resetState() {
        nodeOffsets.clear();
        bwdEdges.clear();
    }
    inline void addEdge(common::offset_t boundOffset, common::offset_t nbrOffset) {
        if (!bwdEdges.contains(nbrOffset)) {
            nodeOffsets.push_back(nbrOffset);
            bwdEdges.insert({nbrOffset, std::vector<common::offset_t>{}});
        }
        bwdEdges.at(nbrOffset).push_back(boundOffset);
    }
};

struct BaseBFSMorsel {
    friend struct FixedLengthPathScanner;
    // Static information
    common::offset_t maxOffset;
    uint8_t lowerBound;
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
    std::unordered_set<common::offset_t> targetDstNodeOffsets;

    explicit BaseBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : maxOffset{maxOffset}, lowerBound{lowerBound}, upperBound{upperBound}, currentLevel{0},
          nextNodeIdxToExtend{0}, numTargetDstNodes{0} {
        if (semiMask->isEnabled()) {
            for (auto offset = 0u; offset < getNumNodes(); ++offset) {
                if (semiMask->isNodeMasked(offset)) {
                    targetDstNodeOffsets.insert(offset);
                }
            }
        }
    }
    virtual ~BaseBFSMorsel() = default;

    // Get next node offset to extend from current level.
    common::offset_t getNextNodeOffset();

    virtual void resetState();
    virtual bool isComplete() = 0;
    virtual void markSrc(common::offset_t offset) = 0;
    virtual void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset) = 0;
    inline void finalizeCurrentLevel() { moveNextLevelAsCurrentLevel(); }

protected:
    inline uint64_t getNumNodes() const { return maxOffset + 1; }
    inline bool isAllDstTarget() const { return targetDstNodeOffsets.empty(); }
    inline bool isCurrentFrontierEmpty() const { return currentFrontier->nodeOffsets.empty(); }
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
    void moveNextLevelAsCurrentLevel();
};

struct ShortestPathBFSMorsel : public BaseBFSMorsel {
    // Visited state
    uint64_t numVisitedDstNodes;
    uint8_t* visitedNodes;

    ShortestPathBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask}, numVisitedDstNodes{0} {
        visitedNodesBuffer = std::make_unique<uint8_t[]>(getNumNodes() * sizeof(uint8_t));
        visitedNodes = visitedNodesBuffer.get();
    }

    inline bool isComplete() override {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
    }
    inline void resetState() override {
        BaseBFSMorsel::resetState();
        resetVisitedState();
    }
    void markSrc(common::offset_t offset) override;
    void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset) override;

private:
    inline bool isAllDstReached() const { return numVisitedDstNodes == numTargetDstNodes; }
    void resetVisitedState();

private:
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
};

struct VariableLengthBFSMorsel : public BaseBFSMorsel {
    explicit VariableLengthBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound,
        uint8_t upperBound, NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask} {}

    inline void resetState() override {
        BaseBFSMorsel::resetState();
        numTargetDstNodes = isAllDstTarget() ? getNumNodes() : targetDstNodeOffsets.size();
    }
    inline bool isComplete() override { return isCurrentFrontierEmpty() || isUpperBoundReached(); }
    inline void markSrc(common::offset_t offset) override {
        currentFrontier->nodeOffsets.push_back(offset);
    }
    inline void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset) override {
        nextFrontier->addEdge(boundOffset, nbrOffset);
    }
};

} // namespace processor
} // namespace kuzu
