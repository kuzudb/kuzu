#include "processor/operator/scan/scan_multi_node_tables.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ScanMultiNodeTables::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [id, info] : infos) {
        auto readState = std::make_unique<storage::NodeTableReadState>(info->columnIDs);
        ScanTable::initVectors(*readState, *resultSet);
        readStates.insert({id, std::move(readState)});
    }
}

bool ScanMultiNodeTables::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto pos = nodeIDVector->state->getSelVector()[0];
    auto tableID = nodeIDVector->getValue<nodeID_t>(pos).tableID;
    KU_ASSERT(readStates.contains(tableID) && infos.contains(tableID));
    auto info = infos.at(tableID).get();
    info->table->initializeReadState(context->clientContext->getTx(), info->columnIDs,
        *readStates[tableID]);
    info->table->read(context->clientContext->getTx(), *readStates.at(tableID));
    return true;
}

std::unique_ptr<PhysicalOperator> ScanMultiNodeTables::clone() {
    common::table_id_map_t<std::unique_ptr<ScanNodeTableInfo>> clonedInfos;
    for (auto& [id, info] : infos) {
        clonedInfos.insert({id, info->copy()});
    }
    return make_unique<ScanMultiNodeTables>(nodeIDPos, outVectorsPos, std::move(clonedInfos),
        children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
