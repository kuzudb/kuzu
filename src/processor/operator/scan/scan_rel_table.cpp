#include "processor/operator/scan/scan_rel_table.h"

#include "binder/expression/expression_util.h"
#include "processor/execution_context.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string ScanRelTablePrintInfo::toString() const {
    std::string result = "Tables: ";
    for (auto& tableName : tableNames) {
        result += tableName;
        if (tableName != tableNames.back()) {
            result += ", ";
        }
    }
    if (!alias.empty()) {
        result += ",Alias: ";
        result += alias;
    }
    if (!properties.empty()) {
        result += ",Properties: ";
        result += binder::ExpressionUtil::toString(properties);
    }
    return result;
}

void ScanRelTableSharedState::initialize(const transaction::Transaction* transaction,
    RelTable* table, ScanRelTableProgressSharedState& progressSharedState) {
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

void ScanRelTableSharedState::nextMorsel(RelTableScanState& scanState,
    ScanRelTableProgressSharedState& progressSharedState) {
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

table_id_map_t<SemiMask*> ScanRelTable::getSemiMasks() const {
    table_id_map_t<SemiMask*> result;
    KU_ASSERT(relInfos.size() == sharedStates.size());
    for (auto i = 0u; i < sharedStates.size(); ++i) {
        result.insert({relInfos[i].table->getTableID(), sharedStates[i]->getSemiMask()});
    }
    return result;
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    std::vector<ValueVector*> outVectors;
    for (auto& pos : info.outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
    auto nodeIDVector = resultSet->getValueVector(info.nodeIDPos);
    scanState =
        std::make_unique<RelTableScanState>(nodeIDVector.get(), outVectors, nodeIDVector->state);
    KU_ASSERT(relInfos.size() >= 1 && sharedStates.size() >= 1);
    auto& firstNodeInfo = relInfos[0];
    scanState->setToTable(context->clientContext->getTransaction(), firstNodeInfo.table,
        firstNodeInfo.columnIDs, copyVector(firstNodeInfo.columnPredicates));
    scanState->semiMask = sharedStates[0]->getSemiMask();
}

void ScanRelTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == relInfos.size());
    for (auto i = 0u; i < relInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTransaction(),
            relInfos[i].table->ptrCast<RelTable>(), *progressSharedState);
    }
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTransaction();
    while (currentTableIdx < relInfos.size()) {
        const auto& info = relInfos[currentTableIdx];
        while (info.table->scan(transaction, *scanState)) {
            const auto outputSize = scanState->outState->getSelVector().getSelSize();
            if (outputSize > 0) {
                scanState->outState->setToUnflat();
                metrics->numOutputTuple.increase(outputSize);
                return true;
            }
        }
        sharedStates[currentTableIdx]->nextMorsel(*scanState, *progressSharedState);
        if (scanState->source == TableScanSource::NONE) {
            currentTableIdx++;
            if (currentTableIdx < relInfos.size()) {
                auto& currentInfo = relInfos[currentTableIdx];
                scanState->setToTable(transaction, currentInfo.table, currentInfo.columnIDs,
                    copyVector(currentInfo.columnPredicates));
                scanState->semiMask = sharedStates[currentTableIdx]->getSemiMask();
            }
        } else {
            info.table->initScanPropertiesState(transaction, *scanState);
        }
    }
    return false;
}

double ScanRelTable::getProgress(ExecutionContext* /*context*/) const {
    if (currentTableIdx >= relInfos.size()) {
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
