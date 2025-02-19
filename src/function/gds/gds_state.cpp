#include "function/gds/gds_state.h"

namespace kuzu {
namespace function {

void GDSComputeState::initSource(common::nodeID_t sourceNodeID) const {
    frontierPair->initSource(sourceNodeID);
    auxiliaryState->initSource(sourceNodeID);
}

void GDSComputeState::beginFrontierCompute(common::table_id_t currTableID,
    common::table_id_t nextTableID) const {
    frontierPair->beginFrontierComputeBetweenTables(currTableID, nextTableID);
    auxiliaryState->beginFrontierCompute(currTableID, nextTableID);
}

common::table_id_set_t GDSComputeState::getActiveRelTableIDs(size_t index, graph::Graph* graph)  {
    if (stepActiveRelTableIDs.empty()) {
        auto nodeIDs = graph->getRelTableIDs();
        common::table_id_set_t set;
        set.insert(nodeIDs.begin(), nodeIDs.end());
        stepActiveRelTableIDs.push_back(set);
    }
    if (index < stepActiveRelTableIDs.size()) {
        return stepActiveRelTableIDs[index];
    } else {
        return stepActiveRelTableIDs.back();
    }
}
} // namespace function
} // namespace kuzu
