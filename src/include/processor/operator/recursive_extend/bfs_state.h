#pragma once

#include "frontier.h"
#include "processor/operator/mask.h"

namespace kuzu {
namespace processor {

struct BaseBFSMorsel {
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
    virtual void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset,
        common::offset_t relOffset, uint64_t multiplicity) = 0;

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

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

template<bool trackPath>
struct ShortestPathMorsel : public BaseBFSMorsel {
    // Visited state
    uint64_t numVisitedDstNodes;
    uint8_t* visitedNodes;

    ShortestPathMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask}, numVisitedDstNodes{0} {
        visitedNodesBuffer = std::make_unique<uint8_t[]>(getNumNodes() * sizeof(uint8_t));
        visitedNodes = visitedNodesBuffer.get();
    }
    ~ShortestPathMorsel() override = default;

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
    }
    inline void resetState() final {
        BaseBFSMorsel::resetState();
        resetVisitedState();
    }

    inline void markSrc(common::offset_t offset) final {
        if (visitedNodes[offset] == NOT_VISITED_DST) {
            visitedNodes[offset] = VISITED_DST;
            numVisitedDstNodes++;
        }
        currentFrontier->addNode(offset);
    }

    inline void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset,
        common::offset_t relOffset, uint64_t multiplicity) final {
        if (visitedNodes[nbrOffset] == NOT_VISITED_DST) {
            visitedNodes[nbrOffset] = VISITED_DST;
            numVisitedDstNodes++;
            if constexpr (trackPath) {
                nextFrontier->addEdge(boundOffset, nbrOffset, relOffset);
            } else {
                nextFrontier->addNode(nbrOffset);
            }
        } else if (visitedNodes[nbrOffset] == NOT_VISITED) {
            visitedNodes[nbrOffset] = VISITED;
            if constexpr (trackPath) {
                nextFrontier->addEdge(boundOffset, nbrOffset, relOffset);
            } else {
                nextFrontier->addNode(nbrOffset);
            }
        }
    }

protected:
    inline bool isAllDstReached() const { return numVisitedDstNodes == numTargetDstNodes; }
    inline void resetVisitedState() {
        numVisitedDstNodes = 0;
        if (!isAllDstTarget()) {
            std::fill(
                visitedNodes, visitedNodes + getNumNodes(), (uint8_t)VisitedState::NOT_VISITED);
            for (auto offset : targetDstNodeOffsets) {
                visitedNodes[offset] = VisitedState::NOT_VISITED_DST;
            }
            numTargetDstNodes = targetDstNodeOffsets.size();
        } else {
            std::fill(
                visitedNodes, visitedNodes + getNumNodes(), (uint8_t)VisitedState::NOT_VISITED_DST);
            numTargetDstNodes = getNumNodes();
        }
    }

private:
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
};

template<bool trackPath>
struct VariableLengthMorsel : public BaseBFSMorsel {
    VariableLengthMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask} {}
    ~VariableLengthMorsel() override = default;

    inline void resetState() final {
        BaseBFSMorsel::resetState();
        numTargetDstNodes = isAllDstTarget() ? getNumNodes() : targetDstNodeOffsets.size();
    }
    inline bool isComplete() final { return isCurrentFrontierEmpty() || isUpperBoundReached(); }

    inline void markSrc(common::offset_t offset) final {
        currentFrontier->addNodeWithMultiplicity(offset, 1 /* multiplicity */);
    }

    inline void markVisited(common::offset_t boundOffset, common::offset_t nbrOffset,
        common::offset_t relOffset, uint64_t multiplicity) final {
        if constexpr (trackPath) {
            nextFrontier->addEdge(boundOffset, nbrOffset, relOffset);
        } else {
            nextFrontier->addNodeWithMultiplicity(nbrOffset, multiplicity);
        }
    }
};

} // namespace processor
} // namespace kuzu
