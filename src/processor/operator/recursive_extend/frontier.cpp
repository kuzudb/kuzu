#include "processor/operator/recursive_extend/frontier.h"

namespace kuzu {
namespace processor {

void Frontier::addEdge(
    common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID, common::nodeID_t relID) {
    if (!bwdEdges.contains(nbrNodeID)) {
        nodeIDs.push_back(nbrNodeID);
        bwdEdges.insert({nbrNodeID, std::vector<frontier::node_rel_id_t>{}});
    }
    bwdEdges.at(nbrNodeID).emplace_back(boundNodeID, relID);
}

void Frontier::addNodeWithMultiplicity(common::nodeID_t nodeID, uint64_t multiplicity) {
    if (nodeIDToMultiplicity.contains(nodeID)) {
        nodeIDToMultiplicity.at(nodeID) += multiplicity;
    } else {
        nodeIDToMultiplicity.insert({nodeID, multiplicity});
        nodeIDs.push_back(nodeID);
    }
}

} // namespace processor
} // namespace kuzu
