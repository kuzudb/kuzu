#pragma once

#include <unordered_map>

#include "common/types/types_include.h"
#include "function/hash/hash_operations.h"

namespace kuzu {
namespace processor {

namespace frontier {
using node_rel_id_t = std::pair<common::nodeID_t, common::relID_t>;
struct InternalIDHasher {
    std::size_t operator()(const common::internalID_t& internalID) const {
        common::hash_t result;
        function::operation::Hash::operation<common::internalID_t>(internalID, result);
        return result;
    }
};
using node_id_set_t = std::unordered_set<common::nodeID_t, InternalIDHasher>;
template<typename T>
using node_id_map_t = std::unordered_map<common::nodeID_t, T, InternalIDHasher>;
} // namespace frontier

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

    inline void addNode(common::nodeID_t nodeID) { nodeIDs.push_back(nodeID); }

    void addEdge(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID, common::nodeID_t relID);

    void addNodeWithMultiplicity(common::nodeID_t nodeID, uint64_t multiplicity);

    inline uint64_t getMultiplicity(common::nodeID_t nodeID) const {
        return nodeIDToMultiplicity.empty() ? 1 : nodeIDToMultiplicity.at(nodeID);
    }

public:
    std::vector<common::nodeID_t> nodeIDs;
    frontier::node_id_map_t<std::vector<frontier::node_rel_id_t>> bwdEdges;
    frontier::node_id_map_t<uint64_t> nodeIDToMultiplicity;
};

} // namespace processor
} // namespace kuzu
