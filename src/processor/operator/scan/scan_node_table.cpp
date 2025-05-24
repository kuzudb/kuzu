#include "processor/operator/scan/scan_node_table.h"

#include "binder/expression/expression_util.h"
#include "processor/execution_context.h"
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

void ScanNodeTableSharedState::initialize(const transaction::Transaction* transaction,
    NodeTable* table, ScanNodeTableProgressSharedState& progressSharedState) {
    this->table = table;
    this->currentCommittedGroupIdx = 0;
    this->currentUnCommittedGroupIdx = 0;
    this->numCommittedNodeGroups = table->getNumCommittedNodeGroups();
    if (transaction->isWriteTransaction()) {
        if (const auto localTable =
                transaction->getLocalStorage()->getLocalTable(this->table->getTableID())) {
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

table_id_map_t<SemiMask*> ScanNodeTable::getSemiMasks() const {
    table_id_map_t<SemiMask*> result;
    KU_ASSERT(nodeInfos.size() == sharedStates.size());
    for (auto i = 0u; i < sharedStates.size(); ++i) {
        result.insert({nodeInfos[i].table->getTableID(), sharedStates[i]->getSemiMask()});
    }
    return result;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    std::vector<ValueVector*> outVectors;
    for (auto& pos : info.outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
    auto nodeIDVector = resultSet->getValueVector(info.nodeIDPos);
    scanState =
        std::make_unique<NodeTableScanState>(nodeIDVector.get(), outVectors, nodeIDVector->state);
    KU_ASSERT(nodeInfos.size() >= 1 && sharedStates.size() >= 1);
    auto& firstNodeInfo = nodeInfos[0];
    scanState->setToTable(context->clientContext->getTransaction(), firstNodeInfo.table,
        firstNodeInfo.columnIDs, copyVector(firstNodeInfo.columnPredicates));
    scanState->semiMask = sharedStates[0]->getSemiMask();
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == nodeInfos.size());
    for (auto i = 0u; i < nodeInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTransaction(),
            nodeInfos[i].table->ptrCast<NodeTable>(), *progressSharedState);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    const auto transaction = context->clientContext->getTransaction();
    while (currentTableIdx < nodeInfos.size()) {
        const auto& info = nodeInfos[currentTableIdx];
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
            if (currentTableIdx < nodeInfos.size()) {
                auto& currentInfo = nodeInfos[currentTableIdx];
                scanState->setToTable(transaction, currentInfo.table, currentInfo.columnIDs,
                    copyVector(currentInfo.columnPredicates));
                scanState->semiMask = sharedStates[currentTableIdx]->getSemiMask();
            }
        } else {
            info.table->initScanState(transaction, *scanState);
        }
    }
    return false;
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
