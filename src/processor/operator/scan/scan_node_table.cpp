#include "processor/operator/scan/scan_node_table.h"

#include "binder/expression/expression_util.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string ScanNodeTablePrintInfo::toString() const {
    std::string result = "Tables: ";
    for (auto& tableName : tableNames) {
        result += tableName;
        if (tableName != tableNames.back()) {
            result += ", ";
        }
    }
    if (!properties.empty()) {
        result += ",Properties: ";
        result += binder::ExpressionUtil::toString(properties);
    }
    return result;
}

void ScanNodeTableSharedState::initialize(const transaction::Transaction* transaction,
    NodeTable* table, ScanNodeTableProgressSharedState& progressSharedState) {
    this->table = table;
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
    progressSharedState.numGroups += numCommittedNodeGroups;
}

void ScanNodeTableSharedState::nextMorsel(NodeTableScanState& scanState,
    ScanNodeTableProgressSharedState& progressSharedState) {
    std::unique_lock lck{mtx};
    if (currentCommittedGroupIdx < numCommittedNodeGroups) {
        scanState.nodeGroupIdx = currentCommittedGroupIdx++;
        progressSharedState.numGroupsScanned++;
        scanState.source = TableScanSource::COMMITTED;
        return;
    }
    if (currentUnCommittedGroupIdx < numUnCommittedNodeGroups) {
        scanState.nodeGroupIdx = currentUnCommittedGroupIdx++;
        scanState.source = TableScanSource::UNCOMMITTED;
        return;
    }
    scanState.source = TableScanSource::NONE;
}

void ScanNodeTableInfo::initScanState(NodeSemiMask* semiMask) {
    std::vector<const Column*> columns;
    columns.reserve(columnIDs.size());
    for (const auto columnID : columnIDs) {
        if (columnID == INVALID_COLUMN_ID) {
            columns.push_back(nullptr);
        } else {
            columns.push_back(&table->getColumn(columnID));
        }
    }
    localScanState = std::make_unique<NodeTableScanState>(table->getTableID(), columnIDs, columns,
        copyVector(columnPredicates));
    localScanState->semiMask = semiMask;
}

std::vector<NodeSemiMask*> ScanNodeTable::getSemiMasks() const {
    std::vector<NodeSemiMask*> result;
    for (auto& sharedState : sharedStates) {
        result.push_back(sharedState->getSemiMask());
    }
    return result;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto i = 0u; i < nodeInfos.size(); ++i) {
        auto& nodeInfo = nodeInfos[i];
        nodeInfo.initScanState(sharedStates[i]->getSemiMask());
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

void ScanNodeTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    state.rowIdxVector->state = state.nodeIDVector->state;
    state.outState = state.rowIdxVector->state.get();
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == nodeInfos.size());
    for (auto i = 0u; i < nodeInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTx(), nodeInfos[i].table,
            *progressSharedState);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTx();
    while (currentTableIdx < nodeInfos.size()) {
        const auto& info = nodeInfos[currentTableIdx];
        auto& scanState = *info.localScanState;
        while (info.table->scan(transaction, scanState)) {
            const auto outputSize = scanState.outState->getSelVector().getSelSize();
            if (outputSize > 0) {
                scanState.outState->setToUnflat();
                metrics->numOutputTuple.increase(outputSize);
                return true;
            }
        }
        sharedStates[currentTableIdx]->nextMorsel(scanState, *progressSharedState);
        if (scanState.source == TableScanSource::NONE) {
            currentTableIdx++;
        } else {
            info.table->initScanState(transaction, scanState);
        }
    }
    return false;
}

std::unique_ptr<PhysicalOperator> ScanNodeTable::clone() {
    return make_unique<ScanNodeTable>(info.copy(), copyVector(nodeInfos), sharedStates, id,
        printInfo->copy(), progressSharedState);
}

double ScanNodeTable::getProgress(ExecutionContext* /*context*/) const {
    if (currentTableIdx >= nodeInfos.size()) {
        return 1.0;
    }
    if (progressSharedState->numGroups == 0) {
        return 0.0;
    }
    return static_cast<double>(progressSharedState->numGroupsScanned) /
           progressSharedState->numGroups;
}

} // namespace processor
} // namespace kuzu
