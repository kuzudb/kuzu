#include "processor/operator/scan/offset_scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void OffsetScanNodeTable::init(common::nodeID_t nodeID) {
    nodeIDVector->setValue<nodeID_t>(0, nodeID);
    executed = false;
}

void OffsetScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, nodeInfo] : tableIDToNodeInfo) {
        nodeInfo.localScanState = std::make_unique<NodeTableScanState>(nodeInfo.columnIDs);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

bool OffsetScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (executed) {
        return false;
    }
    executed = true;
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
