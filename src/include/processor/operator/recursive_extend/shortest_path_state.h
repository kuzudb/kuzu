#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

// TODO:remove
struct VisitedNodeOffsets {
    uint8_t* visitedOffsets;
    uint64_t numNodes;

    VisitedNodeOffsets(common::offset_t maxOffset) : numNodes{maxOffset + 1} {
        visitedOffsetsBuffer = std::make_unique<uint8_t[]>(numNodes * sizeof(uint8_t));
        visitedOffsets = visitedOffsetsBuffer.get();
    }

private:
    std::unique_ptr<uint8_t[]> visitedOffsetsBuffer;
};

template<bool TRACK_PATH>
struct ShortestPathState : public BaseBFSState {
    // Visited state
    uint64_t numVisitedDstNodes;
    std::unordered_map<common::table_id_t, std::unique_ptr<VisitedNodeOffsets>> visitedNodes;

    ShortestPathState(uint8_t upperBound, const std::vector<NodeOffsetSemiMask*>& semiMasks,
        transaction::Transaction* trx)
        : BaseBFSState{upperBound, semiMasks, trx}, numVisitedDstNodes{0} {
        for (auto semiMask : semiMasks) {
            auto nodeTable = semiMask->getNodeTable();
            auto maxNodeOffset = nodeTable->getMaxNodeOffset(trx);
            visitedNodes.insert(
                {nodeTable->getTableID(), std::make_unique<VisitedNodeOffsets>(maxNodeOffset)});
        }
    }
    ~ShortestPathState() override = default;

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
    }
    inline void resetState() final {
        BaseBFSState::resetState();
        resetVisitedState();
    }

    inline void markSrc(common::nodeID_t nodeID) final {
        auto visitedOffsets = visitedNodes.at(nodeID.tableID)->visitedOffsets;
        if (visitedOffsets[nodeID.offset] == NOT_VISITED_DST) {
            visitedOffsets[nodeID.offset] = VISITED_DST;
            numVisitedDstNodes++;
        }
        currentFrontier->addNode(nodeID);
    }

    inline void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::nodeID_t relID, uint64_t multiplicity) final {
        auto visitedOffsets = visitedNodes.at(nbrNodeID.tableID)->visitedOffsets;
        if (visitedOffsets[nbrNodeID.offset] == NOT_VISITED_DST) {
            visitedOffsets[nbrNodeID.offset] = VISITED_DST;
            numVisitedDstNodes++;
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNode(nbrNodeID);
            }
        } else if (visitedOffsets[nbrNodeID.offset] == NOT_VISITED) {
            visitedOffsets[nbrNodeID.offset] = VISITED;
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNode(nbrNodeID);
            }
        }
    }

    inline bool isAllDstReached() const { return numVisitedDstNodes == numTargetDstNodes; }
    inline void resetVisitedState() {
        numVisitedDstNodes = 0;
        if (!targetDstNodeIDs.empty()) {
            for (auto& [_, visitedNode] : visitedNodes) {
                std::fill(visitedNode->visitedOffsets,
                    visitedNode->visitedOffsets + visitedNode->numNodes,
                    (uint8_t)VisitedState::NOT_VISITED);
            }
            for (auto& nodeID : targetDstNodeIDs) {
                visitedNodes.at(nodeID.tableID)->visitedOffsets[nodeID.offset] =
                    VisitedState::NOT_VISITED_DST;
            }
        } else {
            for (auto& [_, visitedNode] : visitedNodes) {
                std::fill(visitedNode->visitedOffsets,
                    visitedNode->visitedOffsets + visitedNode->numNodes,
                    (uint8_t)VisitedState::NOT_VISITED_DST);
            }
        }
    }
};

} // namespace processor
} // namespace kuzu
