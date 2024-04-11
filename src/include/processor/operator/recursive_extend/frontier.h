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

} // namespace processor
} // namespace kuzu
