#include "processor/operator/scan/offset_scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void OffsetScanNodeTable::init(nodeID_t nodeID) {
    IDVector->setValue<nodeID_t>(0, nodeID);
    executed = false;
}

void OffsetScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, nodeInfo] : tableIDToNodeInfo) {
        nodeInfo.initScanState(nullptr);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

bool OffsetScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (executed) {
        return false;
    }
    executed = true;
    auto transaction = context->clientContext->getTx();
    auto nodeID = IDVector->getValue<nodeID_t>(0);
    KU_ASSERT(tableIDToNodeInfo.contains(nodeID.tableID));
    auto& nodeInfo = tableIDToNodeInfo.at(nodeID.tableID);
    if (nodeID.offset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        nodeInfo.localScanState->source = TableScanSource::UNCOMMITTED;
        nodeInfo.localScanState->nodeGroupIdx =
            StorageUtils::getNodeGroupIdx(nodeID.offset - StorageConstants::MAX_NUM_ROWS_IN_TABLE);
    } else {
        nodeInfo.localScanState->source = TableScanSource::COMMITTED;
        nodeInfo.localScanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeID.offset);
    }
    nodeInfo.table->initializeScanState(transaction, *nodeInfo.localScanState);
    if (!nodeInfo.table->lookup(transaction, *nodeInfo.localScanState)) {
        // LCOV_EXCL_START
        throw RuntimeException(stringFormat("Cannot perform lookup on {}. This should not happen.",
            TypeUtils::toString(nodeID)));
        // LCOV_EXCL_STOP
    }
    return true;
}

} // namespace processor
} // namespace kuzu
