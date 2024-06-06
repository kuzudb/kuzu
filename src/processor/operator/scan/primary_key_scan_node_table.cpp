#include "processor/operator/scan/primary_key_scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

common::idx_t PrimaryKeyScanSharedState::getTableIdx() {
    std::unique_lock lck{mtx};
    if (cursor < numTables) {
        return cursor++;
    }
    return numTables;
}

void PrimaryKeyScanNodeTable::initLocalStateInternal(ResultSet* resultSet,
    ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& nodeInfo : nodeInfos) {
        nodeInfo.localScanState = std::make_unique<NodeTableScanState>(nodeInfo.columnIDs);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
    indexEvaluator->init(*resultSet, context->clientContext);
}

bool PrimaryKeyScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto tableIdx = sharedState->getTableIdx();
    if (tableIdx >= nodeInfos.size()) {
        return false;
    }
    KU_ASSERT(tableIdx < nodeInfos.size());
    auto& nodeInfo = nodeInfos[tableIdx];

    indexEvaluator->evaluate();
    auto indexVector = indexEvaluator->resultVector.get();
    auto& selVector = indexVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    auto pos = selVector.getSelectedPositions()[0];
    if (indexVector->isNull(pos)) {
        return false;
    }

    offset_t nodeOffset;
    bool lookupSucceed =
        nodeInfo.table->getPKIndex()->lookup(transaction, indexVector, pos, nodeOffset);
    if (!lookupSucceed) {
        return false;
    }
    auto nodeID = nodeID_t{nodeOffset, nodeInfo.table->getTableID()};
    nodeInfo.localScanState->nodeIDVector->setValue<nodeID_t>(pos, nodeID);
    nodeInfo.localScanState->source = TableScanSource::COMMITTED;
    nodeInfo.localScanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    nodeInfo.table->initializeScanState(transaction, *nodeInfo.localScanState);
    nodeInfo.table->lookup(transaction, *nodeInfo.localScanState);
    return true;
}

} // namespace processor
} // namespace kuzu
