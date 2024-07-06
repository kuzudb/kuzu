#pragma once

#include "common/types/internal_id_t.h"
#include "common/types/internal_id_util.h"

namespace kuzu {
namespace processor {

using node_rel_id_t = std::pair<common::nodeID_t, common::relID_t>;

/*
 * A Frontier can stores dst node offsets, its multiplicity and its bwd edges. Note that we don't
 * need to track all information in BFS computation.
 *
 * Computation                   |  Information tracked
 * Shortest path track path      |  nodeIDs & bwdEdges
 * Shortest path NOT track path  |  nodeIDs
 * Var length track path         |  nodeIDs & bwdEdges
 * Var length NOT track path     |  nodeIDs & nodeIDToMultiplicity
 */
class Frontier {
public:
    inline void resetState() {
        nodeIDs.clear();
        bwdEdges.clear();
        nodeIDToMultiplicity.clear();
    }

    void addEdge(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID, common::nodeID_t relID);

    void addNodeWithMultiplicity(common::nodeID_t nodeID, uint64_t multiplicity);

    inline uint64_t getMultiplicity(common::nodeID_t nodeID) const {
        return nodeIDToMultiplicity.empty() ? 1 : nodeIDToMultiplicity.at(nodeID);
    }

public:
    std::vector<common::nodeID_t> nodeIDs;
    common::node_id_map_t<std::vector<node_rel_id_t>> bwdEdges;
    common::node_id_map_t<uint64_t> nodeIDToMultiplicity;
};

// We assume number of edges per table is smaller than 2^63. So we mask the highest bit of rel
// offset to indicate if the src and dst node this relationship need to be flipped.
struct RelIDMasker {
    static constexpr uint64_t FLIP_SRC_DST_MASK = 0x8000000000000000;
    static constexpr uint64_t CLEAR_FLIP_SRC_DST_MASK = 0x7FFFFFFFFFFFFFFF;

    static void markFlip(common::internalID_t& relID) { relID.offset |= FLIP_SRC_DST_MASK; }
    static bool needFlip(common::internalID_t& relID) { return relID.offset & FLIP_SRC_DST_MASK; }
    static void clearMark(common::internalID_t& relID) { relID.offset &= CLEAR_FLIP_SRC_DST_MASK; }
};

} // namespace processor
} // namespace kuzu
