#include "processor/operator/scan/scan_node_table.h"

#include "storage/local_storage/local_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanNodeTableSharedState::initialize(transaction::Transaction* transaction, NodeTable* table) {
    this->table = table;
    this->currentCommittedGroupIdx = 0;
    this->currentUnCommittedGroupIdx = 0;
    this->numCommittedNodeGroups = table->getNumCommittedNodeGroups();
    if (transaction->isWriteTransaction()) {
        if (const auto localTable = transaction->getLocalStorage()->getLocalTable(
                this->table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
            localNodeGroups = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable)
                                  ->getTableData()
                                  ->getNodeGroups();
        }
    }
}

void ScanNodeTableSharedState::nextMorsel(NodeTableScanState& scanState) {
    std::unique_lock lck{mtx};
    if (currentCommittedGroupIdx < numCommittedNodeGroups) {
        scanState.nodeGroupIdx = currentCommittedGroupIdx++;
        scanState.source = TableScanSource::COMMITTED;
        return;
    }
    if (currentUnCommittedGroupIdx < localNodeGroups.size()) {
        scanState.localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(
            localNodeGroups[currentUnCommittedGroupIdx++]);
        scanState.source = TableScanSource::UNCOMMITTED;
        return;
    }
    scanState.source = TableScanSource::NONE;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& nodeInfo : nodeInfos) {
        nodeInfo.localScanState = std::make_unique<NodeTableScanState>(nodeInfo.columnIDs);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == nodeInfos.size());
    for (auto i = 0u; i < nodeInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTx(), nodeInfos[i].table);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    while (currentTableIdx < nodeInfos.size()) {
        const auto& info = nodeInfos[currentTableIdx];
        auto& scanState = *info.localScanState;
        while (scanState.source != TableScanSource::NONE &&
               info.table->scan(context->clientContext->getTx(), scanState)) {
            if (scanState.nodeIDVector->state->getSelVector().getSelSize() > 0) {
                return true;
            }
        }
        sharedStates[currentTableIdx]->nextMorsel(scanState);
        if (scanState.source == TableScanSource::NONE) {
            currentTableIdx++;
        } else {
            info.table->initializeScanState(context->clientContext->getTx(), scanState);
        }
    }
    return false;
}

std::unique_ptr<PhysicalOperator> ScanNodeTable::clone() {
    return make_unique<ScanNodeTable>(info.copy(), copyVector(nodeInfos), sharedStates, id,
        paramsString);
}

} // namespace processor
} // namespace kuzu
