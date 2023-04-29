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

    Frontier() = default;
    virtual ~Frontier() = default;
    inline virtual void resetState() { nodeOffsets.clear(); }
    inline virtual uint64_t getMultiplicity(common::offset_t offset) { return 1; }
};

struct FrontierWithMultiplicity : public Frontier {
    // Multiplicity stands for number of paths that can reach an offset.
    std::unordered_map<common::offset_t, uint64_t> offsetToMultiplicity;

    FrontierWithMultiplicity() : Frontier() {}
    inline void resetState() override {
        Frontier::resetState();
        offsetToMultiplicity.clear();
    }
    inline uint64_t getMultiplicity(common::offset_t offset) override {
        assert(offsetToMultiplicity.contains(offset));
        return offsetToMultiplicity.at(offset);
    }
    inline void addOffset(common::offset_t offset, uint64_t multiplicity) {
        if (offsetToMultiplicity.contains(offset)) {
            offsetToMultiplicity.at(offset) += multiplicity;
        } else {
            offsetToMultiplicity.insert({offset, multiplicity});
            nodeOffsets.push_back(offset);
        }
    }
    inline bool contains(common::offset_t offset) const {
        return offsetToMultiplicity.contains(offset);
    }
};

struct BaseBFSMorsel {
    // Static information
    common::offset_t maxOffset;
    uint8_t lowerBound;
    uint8_t upperBound;
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    std::unique_ptr<Frontier> currentFrontier;
    std::unique_ptr<Frontier> nextFrontier;
    // Target information.
    // Target dst nodes are populated from semi mask and is expected to have small size.
    // TargetDstNodeOffsets is empty if no semi mask available. Note that at the end of BFS, we may
    // not visit all target dst nodes because they may simply not connect to src.
    uint64_t numTargetDstNodes;
    std::vector<common::offset_t> targetDstNodeOffsets;

    explicit BaseBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : maxOffset{maxOffset}, lowerBound{lowerBound}, upperBound{upperBound}, currentLevel{0},
          nextNodeIdxToExtend{0}, numTargetDstNodes{0} {
        if (semiMask->isEnabled()) {
            for (auto offset = 0u; offset < maxOffset + 1; ++offset) {
                if (semiMask->isNodeMasked(offset)) {
                    targetDstNodeOffsets.push_back(offset);
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
    virtual void markVisited(common::offset_t offset, uint64_t multiplicity) = 0;
    virtual void finalizeCurrentLevel() = 0;

protected:
    inline bool isCurrentFrontierEmpty() const { return currentFrontier->nodeOffsets.empty(); }
    inline bool isUpperBoundReached() const { return currentLevel == upperBound; }
    inline bool isAllDstTarget() const { return targetDstNodeOffsets.empty(); }
    void moveNextLevelAsCurrentLevel();
    virtual std::unique_ptr<Frontier> createFrontier() = 0;
};

struct ShortestPathBFSMorsel : public BaseBFSMorsel {
    // Visited state
    uint64_t numVisitedDstNodes;
    uint8_t* visitedNodes;
    // Results
    std::vector<common::offset_t> dstNodeOffsets;
    std::unordered_map<common::offset_t, uint64_t> dstNodeOffset2PathLength;

    ShortestPathBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound, uint8_t upperBound,
        NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask}, numVisitedDstNodes{0} {
        currentFrontier = std::make_unique<Frontier>();
        nextFrontier = std::make_unique<Frontier>();
        visitedNodesBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
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
    void markVisited(common::offset_t offset, uint64_t multiplicity) override;
    inline void finalizeCurrentLevel() override { moveNextLevelAsCurrentLevel(); }

private:
    inline bool isAllDstReached() const { return numVisitedDstNodes == numTargetDstNodes; }
    void resetVisitedState();
    inline std::unique_ptr<Frontier> createFrontier() override {
        return std::make_unique<Frontier>();
    }

private:
    std::unique_ptr<uint8_t[]> visitedNodesBuffer;
};

struct VariableLengthBFSMorsel : public BaseBFSMorsel {
    // Results
    std::vector<common::offset_t> dstNodeOffsets;
    std::unordered_map<common::offset_t, uint64_t> dstNodeOffset2NumPath;

    explicit VariableLengthBFSMorsel(common::offset_t maxOffset, uint8_t lowerBound,
        uint8_t upperBound, NodeOffsetSemiMask* semiMask)
        : BaseBFSMorsel{maxOffset, lowerBound, upperBound, semiMask} {
        currentFrontier = std::make_unique<FrontierWithMultiplicity>();
        nextFrontier = std::make_unique<FrontierWithMultiplicity>();
    }

    inline void resetState() override {
        BaseBFSMorsel::resetState();
        resetNumPath();
    }
    inline bool isComplete() override { return isCurrentFrontierEmpty() || isUpperBoundReached(); }
    inline void markSrc(common::offset_t offset) override {
        ((FrontierWithMultiplicity&)*currentFrontier).addOffset(offset, 1);
    }
    inline void markVisited(common::offset_t offset, uint64_t multiplicity) override {
        ((FrontierWithMultiplicity&)*nextFrontier).addOffset(offset, multiplicity);
    }
    inline void finalizeCurrentLevel() override {
        moveNextLevelAsCurrentLevel();
        updateNumPathFromCurrentFrontier();
    }

private:
    inline void resetNumPath() {
        dstNodeOffsets.clear();
        dstNodeOffset2NumPath.clear();
        numTargetDstNodes = isAllDstTarget() ? maxOffset + 1 : targetDstNodeOffsets.size();
    }
    inline void updateNumPath(common::offset_t offset, uint64_t numPath) {
        if (!dstNodeOffset2NumPath.contains(offset)) {
            dstNodeOffsets.push_back(offset);
            dstNodeOffset2NumPath.insert({offset, numPath});
        } else {
            dstNodeOffset2NumPath.at(offset) += numPath;
        }
    }
    void updateNumPathFromCurrentFrontier();
    inline std::unique_ptr<Frontier> createFrontier() override {
        return std::make_unique<FrontierWithMultiplicity>();
    }
};

} // namespace processor
} // namespace kuzu
