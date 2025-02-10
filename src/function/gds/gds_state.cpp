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

} // namespace function
} // namespace kuzu
