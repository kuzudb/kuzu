#include "processor/operator/recursive_extend/frontier.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Frontier::addEdge(nodeID_t boundNodeID, nodeID_t nbrNodeID, nodeID_t relID) {
    if (!bwdEdges.contains(nbrNodeID)) {
        nodeIDs.push_back(nbrNodeID);
        bwdEdges.insert({nbrNodeID, std::vector<node_rel_id_t>{}});
    }
    bwdEdges.at(nbrNodeID).emplace_back(boundNodeID, relID);
}

void Frontier::addNodeWithMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
    if (nodeIDToMultiplicity.contains(nodeID)) {
        nodeIDToMultiplicity.at(nodeID) += multiplicity;
    } else {
        nodeIDToMultiplicity.insert({nodeID, multiplicity});
        nodeIDs.push_back(nodeID);
    }
}

} // namespace processor
} // namespace kuzu
