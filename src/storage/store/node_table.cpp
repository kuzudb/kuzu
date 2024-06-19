#include "storage/store/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table_data.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(StorageManager* storageManager, NodeTableCatalogEntry* nodeTableEntry,
    MemoryManager* memoryManager, VirtualFileSystem* vfs, main::ClientContext* context)
    : Table{nodeTableEntry, storageManager->getNodesStatisticsAndDeletedIDs(), memoryManager,
          &storageManager->getWAL()},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyPID())} {
    tableData = std::make_unique<NodeTableData>(storageManager->getDataFH(),
        storageManager->getMetadataDAC(), nodeTableEntry, bufferManager, wal,
        nodeTableEntry->getPropertiesRef(), storageManager->getNodesStatisticsAndDeletedIDs(),
        storageManager->compressionEnabled());
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
}

void NodeTable::initializePKIndex(const std::string& databasePath,
    const NodeTableCatalogEntry* nodeTableEntry, bool readOnly, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    pkIndex = std::make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(vfs, databasePath, tableID), readOnly,
        nodeTableEntry->getPrimaryKey()->getDataType().getPhysicalType(), *bufferManager, wal, vfs,
        context);
}

void NodeTable::initializeScanState(Transaction* transaction, TableScanState& scanState) const {
    switch (scanState.source) {
    case TableScanSource::COMMITTED: {
        tableData->initializeScanState(transaction, scanState);
    } break;
    case TableScanSource::UNCOMMITTED: {
        scanState.cast<NodeTableScanState>().localNodeGroup->initializeScanState(scanState);
    } break;
    default: {
        // DO NOTHING.
    }
    }
}

bool NodeTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    KU_ASSERT(scanState.source != TableScanSource::NONE &&
              scanState.columnIDs.size() == scanState.outputVectors.size());
    for (const auto& outputVector : scanState.outputVectors) {
        (void)outputVector;
        KU_ASSERT(outputVector->state == scanState.nodeIDVector->state);
    }
    auto& nodeScanState = ku_dynamic_cast<TableScanState&, NodeTableScanState&>(scanState);
    auto& dataScanState =
        ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(*scanState.dataScanState);
    if (!dataScanState.nextVector()) {
        return false;
    }
    switch (scanState.source) {
    case TableScanSource::UNCOMMITTED: {
        return scanUnCommitted(nodeScanState);
    }
    case TableScanSource::COMMITTED: {
        return scanCommitted(transaction, nodeScanState);
    }
    default: {
        KU_ASSERT(false);
        return false;
    }
    }
}

void NodeTable::lookup(transaction::Transaction* transaction, TableScanState& scanState) {
    KU_ASSERT(scanState.nodeIDVector->state->getSelVector().getSelSize() == 1);
    tableData->lookup(transaction, *scanState.dataScanState, *scanState.nodeIDVector,
        scanState.outputVectors);
}

bool NodeTable::scanUnCommitted(NodeTableScanState& scanState) {
    scanState.localNodeGroup->scan(scanState);
    return true;
}

bool NodeTable::scanCommitted(Transaction* transaction, NodeTableScanState& scanState) {
    KU_ASSERT(scanState.source == TableScanSource::COMMITTED);
    auto& dataScanState = scanState.dataScanState->cast<NodeDataScanState>();
    // Fill nodeIDVector and set selVector from deleted node offsets.
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx) +
                                 dataScanState.vectorIdx * DEFAULT_VECTOR_CAPACITY;
    if (scanState.semiMask->isEnabled() && !scanState.semiMask->isMasked(startNodeOffset)) {
        scanState.nodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
        return true;
    }
    for (auto i = 0u; i < dataScanState.numRowsToScan; i++) {
        scanState.nodeIDVector->setValue<nodeID_t>(i, {startNodeOffset + i, tableID});
    }
    scanState.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(
        dataScanState.numRowsToScan);
    const auto tableStats =
        getNodeStatisticsAndDeletedIDs()->getNodeStatisticsAndDeletedIDs(transaction, tableID);
    tableStats->setDeletedNodeOffsetsForVector(scanState.nodeIDVector, scanState.nodeGroupIdx,
        dataScanState.vectorIdx, dataScanState.numRowsToScan);

    KU_ASSERT(scanState.source == TableScanSource::COMMITTED);
    tableData->scan(transaction, *scanState.dataScanState, *scanState.nodeIDVector,
        scanState.outputVectors);

    // Scan updates from local storage.
    if (scanState.localNodeGroup) {
        KU_ASSERT(transaction->isWriteTransaction());
        scanState.localNodeGroup->lookup(*scanState.nodeIDVector, scanState.columnIDs,
            scanState.outputVectors);
    }
    return true;
}

offset_t NodeTable::validateUniquenessConstraint(Transaction* transaction,
    const std::vector<ValueVector*>& propertyVectors) const {
    const auto pkVector = propertyVectors[pkColumnID];
    KU_ASSERT(pkVector->state->getSelVector().getSelSize() == 1);
    const auto pkVectorPos = pkVector->state->getSelVector()[0];
    offset_t offset;
    if (pkIndex->lookup(transaction, propertyVectors[pkColumnID], pkVectorPos, offset)) {
        return offset;
    }
    return INVALID_OFFSET;
}

void NodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    const auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    const auto& nodeInsertState =
        ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(insertState);
    KU_ASSERT(nodeInsertState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeInsertState.nodeIDVector.state->getSelVector()[0];
    const auto [offset, isNewNode] = nodesStats->addNode(tableID);
    nodeInsertState.nodeIDVector.setValue(pos, nodeID_t{offset, tableID});
    nodeInsertState.nodeIDVector.setNull(pos, false);
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    insertPK(nodeInsertState.nodeIDVector, nodeInsertState.pkVector);
    if (isNewNode) {
        localTable->insert(insertState);
    } else {
        for (auto columnID = 0u; columnID < getNumColumns(); columnID++) {
            NodeTableUpdateState updateState{columnID, nodeInsertState.nodeIDVector,
                *nodeInsertState.propertyVectors[columnID]};
            localTable->update(updateState);
        }
    }
}

void NodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    const auto& nodeUpdateState =
        ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(updateState);
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    if (nodeUpdateState.columnID == pkColumnID) {
        pkIndex->delete_(nodeUpdateState.pkVector);
        insertPK(nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector);
    }
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->update(updateState);
}

bool NodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& nodeDeleteState =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    if (nodeDeleteState.nodeIDVector.isNull(pos)) {
        return false;
    }
    pkIndex->delete_(&nodeDeleteState.pkVector);
    const auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
        ->deleteNode(tableID, nodeOffset);
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    return localTable->delete_(deleteState);
}

void NodeTable::addColumn(Transaction* transaction, const Property& property,
    ExpressionEvaluator& defaultEvaluator) {
    const auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    nodesStats->addMetadataDAHInfo(tableID, property.getDataType());
    tableData->addColumn(transaction, "", tableData->getColumn(pkColumnID)->getMetadataDA(),
        *nodesStats->getMetadataDAHInfo(transaction, tableID, tableData->getNumColumns()), property,
        defaultEvaluator);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommitNodeGroup(node_group_idx_t nodeGroupIdx, Transaction* transaction,
    LocalNodeNG* localNodeGroup) const {
    tableData->prepareLocalNodeGroupToCommit(nodeGroupIdx, transaction, localNodeGroup);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    pkIndex->prepareCommit();
    const auto localNodeTable = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable);
    tableData->prepareLocalTableToCommit(transaction, localNodeTable->getTableData());
    tableData->prepareCommit();
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommit() {
    pkIndex->prepareCommit();
    tableData->prepareCommit();
}

void NodeTable::prepareRollback(LocalTable* localTable) {
    pkIndex->prepareRollback();
    localTable->clear();
}

void NodeTable::checkpointInMemory() {
    tableData->checkpointInMemory();
    pkIndex->checkpointInMemory();
}

void NodeTable::rollbackInMemory() {
    tableData->rollbackInMemory();
    pkIndex->rollbackInMemory();
}

void NodeTable::insertPK(const ValueVector& nodeIDVector, const ValueVector& pkVector) const {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        const auto nodeIDPos = nodeIDVector.state->getSelVector()[0];
        const auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = pkVector.state->getSelVector()[0];
        if (pkVector.isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(const_cast<ValueVector*>(&pkVector), pkPos, offset)) {
            std::string pkStr;
            TypeUtils::visit(
                pkVector.dataType.getPhysicalType(),
                [&](ku_string_t) { pkStr = pkVector.getValue<ku_string_t>(pkPos).getAsString(); },
                [&pkStr, &pkVector, &pkPos]<typename T>(
                    T) { pkStr = TypeUtils::toString(pkVector.getValue<T>(pkPos)); });
            throw RuntimeException(ExceptionMessage::duplicatePKException(pkStr));
        }
    }
}

} // namespace storage
} // namespace kuzu
