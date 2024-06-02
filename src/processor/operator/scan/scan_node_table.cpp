#include "processor/operator/scan/scan_node_table.h"

#include "storage/local_storage/local_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanNodeTableSharedState::initialize(transaction::Transaction* transaction, NodeTable* table) {
    this->table = table;
    // TODO: Should directly keep pointers to all node groups.
    this->currentCommittedGroupIdx = 0;
    this->currentUnCommittedGroupIdx = 0;
    this->numCommittedNodeGroups = table->getNumCommittedNodeGroups();
    if (transaction->isWriteTransaction()) {
        if (const auto localTable = transaction->getLocalStorage()->getLocalTable(
                this->table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
            auto& localNodeTable = localTable->cast<LocalNodeTable>();
            this->numUnCommittedNodeGroups = localNodeTable.getNumNodeGroups();
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
    // TODO(Guodong): We should split scan of local node table to multiple morsels, too.
    if (currentUnCommittedGroupIdx < numUnCommittedNodeGroups) {
        scanState.nodeGroupIdx = currentCommittedGroupIdx++;
        scanState.source = TableScanSource::UNCOMMITTED;
        return;
    }
    scanState.source = TableScanSource::NONE;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& nodeInfo : nodeInfos) {
        nodeInfo.localScanState =
            std::make_unique<NodeTableScanState>(nodeInfo.table->getTableID(), nodeInfo.columnIDs);
        nodeInfo.localScanState->columnPredicateSets = copyVector(nodeInfo.columnPredicates);
        initVectors(*nodeInfo.localScanState, *resultSet);
        initColumns(*nodeInfo.localScanState, *nodeInfo.table);
    }
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == nodeInfos.size());
    for (auto i = 0u; i < nodeInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTx(), nodeInfos[i].table);
    }
}

void ScanNodeTable::initColumns(NodeTableScanState& scanState, const NodeTable& table) {
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        scanState.columns[i] = table.getColumn(scanState.columnIDs[i]);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTx();
    while (currentTableIdx < nodeInfos.size()) {
        const auto& info = nodeInfos[currentTableIdx];
        auto& scanState = *info.localScanState;
        const auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan) {
            while (scanState.source != TableScanSource::NONE &&
                   info.table->scan(transaction, scanState)) {
                if (scanState.nodeIDVector->state->getSelVector().getSelSize() > 0) {
                    return true;
                }
            }
        }
        sharedStates[currentTableIdx]->nextMorsel(scanState);
        if (scanState.source == TableScanSource::NONE) {
            currentTableIdx++;
        } else {
            info.table->initializeScanState(transaction, scanState);
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
