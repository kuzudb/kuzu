#include "processor/operator/scan/lookup_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void LookupNodeTable::init(common::nodeID_t nodeID) {
    nodeIDVector->setValue<nodeID_t>(0, nodeID);
    hasExecuted = false;
}

void LookupNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, info] : tableIDToNodeInfo) {
        info.localScanState = std::make_unique<NodeTableScanState>(info.columnIDs);
        initVectors(*info.localScanState, *resultSet);
    }
}

bool LookupNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto transaction = context->clientContext->getTx();
    auto nodeID = nodeIDVector->getValue<nodeID_t>(0);
    KU_ASSERT(tableIDToNodeInfo.contains(nodeID.tableID));
    auto& nodeInfo = tableIDToNodeInfo.at(nodeID.tableID);
    // TODO(Guodong): The following lines are probably incorrect.
    nodeInfo.localScanState->source = TableScanSource::COMMITTED;
    nodeInfo.localScanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeID.offset);
    nodeInfo.table->initializeScanState(transaction, *nodeInfo.localScanState);
    nodeInfo.table->lookup(transaction, *nodeInfo.localScanState);
    return true;
}

} // namespace processor
} // namespace kuzu
