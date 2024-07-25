#include "storage/store/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(StorageManager* storageManager, const NodeTableCatalogEntry* nodeTableEntry,
    MemoryManager* memoryManager, VirtualFileSystem* vfs, main::ClientContext* context,
    Deserializer* deSer)
    : Table{nodeTableEntry, storageManager, memoryManager},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyPID())} {
    auto& properties = nodeTableEntry->getPropertiesRef();
    const auto maxColumnID =
        std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
            return a.getColumnID() < b.getColumnID();
        })->getColumnID();
    columns.resize(maxColumnID + 1);
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        const auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[property.getColumnID()] = ColumnFactory::createColumn(columnName,
            property.getDataType().copy(), dataFH, bufferManager, shadowFile, enableCompression);
    }
    nodeGroups = std::make_unique<NodeGroupCollection>(getNodeTableColumnTypes(*this),
        enableCompression, storageManager->getDataFH(), deSer);
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
}

std::unique_ptr<NodeTable> NodeTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    std::string key;
    table_id_t tableID;
    std::string tableName;
    deSer.validateDebuggingInfo(key, "table_id");
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.validateDebuggingInfo(key, "table_name");
    deSer.deserializeValue<std::string>(tableName);
    auto catalogEntry =
        catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID)->ptrCast<NodeTableCatalogEntry>();
    KU_ASSERT(catalogEntry->getName() == tableName);
    return std::make_unique<NodeTable>(storageManager, catalogEntry, memoryManager, vfs, context,
        &deSer);
}

void NodeTable::initializePKIndex(const std::string& databasePath,
    const NodeTableCatalogEntry* nodeTableEntry, bool readOnly, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    pkIndex = std::make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(vfs, databasePath, tableID), readOnly,
        nodeTableEntry->getPrimaryKey()->getDataType().getPhysicalType(), *bufferManager,
        shadowFile, vfs, context);
}

void NodeTable::initializeScanState(Transaction* transaction, TableScanState& scanState) {
    auto& nodeScanState = scanState.cast<NodeTableScanState>();
    switch (nodeScanState.source) {
    case TableScanSource::COMMITTED: {
        nodeScanState.nodeGroup = nodeGroups->getNodeGroup(nodeScanState.nodeGroupIdx);
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        auto& localNodeTable = localTable->cast<LocalNodeTable>();
        nodeScanState.nodeGroup = localNodeTable.getNodeGroup(nodeScanState.nodeGroupIdx);
        KU_ASSERT(nodeScanState.nodeGroup);
    } break;
    default: {
        // DO NOTHING.
    }
    }
    nodeScanState.nodeGroup->initializeScanState(transaction, nodeScanState);
}

bool NodeTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    KU_ASSERT(scanState.source != TableScanSource::NONE &&
              scanState.columns.size() == scanState.outputVectors.size());
    for (const auto& outputVector : scanState.outputVectors) {
        (void)outputVector;
        KU_ASSERT(outputVector->state == scanState.IDVector->state);
    }
    const auto scanResult = scanState.nodeGroup->scan(transaction, scanState);
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    if (scanState.source == TableScanSource::UNCOMMITTED) {
        nodeGroupStartOffset += StorageConstants::MAX_NUM_ROWS_IN_TABLE;
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        scanState.IDVector->setValue(i,
            nodeID_t{nodeGroupStartOffset + scanResult.startRow + i, tableID});
    }
    return true;
}

bool NodeTable::lookup(Transaction* transaction, const TableScanState& scanState) const {
    KU_ASSERT(scanState.IDVector->state->getSelVector().getSelSize() == 1);
    const auto nodeIDPos = scanState.IDVector->state->getSelVector()[0];
    if (scanState.IDVector->isNull(nodeIDPos)) {
        return false;
    }
    const auto nodeOffset = scanState.IDVector->readNodeOffset(nodeIDPos);
    offset_t rowIdxInGroup = INVALID_OFFSET;
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        rowIdxInGroup = nodeOffset - StorageConstants::MAX_NUM_ROWS_IN_TABLE -
                        StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    } else {
        rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    }
    scanState.rowIdxVector->setValue<row_idx_t>(nodeIDPos, rowIdxInGroup);
    return scanState.nodeGroup->lookup(transaction, scanState);
}

offset_t NodeTable::validateUniquenessConstraint(const Transaction* transaction,
    const std::vector<ValueVector*>& propertyVectors) const {
    const auto pkVector = propertyVectors[pkColumnID];
    KU_ASSERT(pkVector->state->getSelVector().getSelSize() == 1);
    const auto pkVectorPos = pkVector->state->getSelVector()[0];
    offset_t offset;
    if (pkIndex->lookup(transaction, propertyVectors[pkColumnID], pkVectorPos, offset,
            [&](offset_t offset_) { return isVisible(transaction, offset_); })) {
        return offset;
    }
    if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL)) {
        return localTable->cast<LocalNodeTable>().validateUniquenessConstraint(transaction,
            *pkVector);
    }
    return INVALID_OFFSET;
}

void NodeTable::validatePkNotExists(const Transaction* transaction, ValueVector* pkVector) {
    common::offset_t dummyOffset;
    auto& selVector = pkVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    if (pkVector->isNull(selVector[0])) {
        throw RuntimeException(ExceptionMessage::nullPKException());
    }
    if (pkIndex->lookup(transaction, pkVector, selVector[0], dummyOffset,
            [&](offset_t offset) { return isVisible(transaction, offset); })) {
        throw RuntimeException(
            ExceptionMessage::duplicatePKException(pkVector->getAsValue(selVector[0])->toString()));
    }
}

void NodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    const auto& nodeInsertState = insertState.cast<NodeTableInsertState>();
    auto& nodeIDSelVector = nodeInsertState.nodeIDVector.state->getSelVector();
    KU_ASSERT(nodeInsertState.propertyVectors[0]->state->getSelVector().getSelSize() == 1);
    KU_ASSERT(nodeIDSelVector.getSelSize() == 1);
    if (nodeInsertState.nodeIDVector.isNull(nodeIDSelVector[0])) {
        return;
    }
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    validatePkNotExists(transaction, (ValueVector*)&nodeInsertState.pkVector);
    localTable->insert(&DUMMY_TRANSACTION, insertState);
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logTableInsertion(tableID, TableType::NODE,
            nodeInsertState.nodeIDVector.state->getSelVector().getSelSize(),
            insertState.propertyVectors);
    }
}

void NodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    auto& nodeUpdateState = updateState.constCast<NodeTableUpdateState>();
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    if (nodeUpdateState.nodeIDVector.isNull(pos)) {
        return;
    }
    if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
        validatePkNotExists(transaction, &nodeUpdateState.propertyVector);
    }
    const auto nodeOffset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->update(&DUMMY_TRANSACTION, updateState);
    } else {
        if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
            insertPK(transaction, nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector);
        }
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups->getNodeGroup(nodeGroupIdx)
            ->update(transaction, rowIdxInGroup, nodeUpdateState.columnID,
                nodeUpdateState.propertyVector);
    }
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logNodeUpdate(tableID, nodeUpdateState.columnID, nodeOffset,
            &nodeUpdateState.propertyVector);
    }
}

bool NodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& nodeDeleteState =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    if (nodeDeleteState.nodeIDVector.isNull(pos)) {
        return false;
    }
    bool isDeleted;
    const auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        isDeleted = localTable->delete_(&DUMMY_TRANSACTION, deleteState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        isDeleted = nodeGroups->getNodeGroup(nodeGroupIdx)->delete_(transaction, rowIdxInGroup);
    }
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logNodeDeletion(tableID, nodeOffset, &nodeDeleteState.pkVector);
    }
    return isDeleted;
}

void NodeTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    auto& property = addColumnState.property;
    KU_ASSERT(property.getColumnID() == columns.size());
    columns.push_back(ColumnFactory::createColumn(property.getName(), property.getDataType().copy(),
        dataFH, bufferManager, shadowFile, enableCompression));
    LocalTable* localTable = nullptr;
    if (transaction->getLocalStorage()) {
        localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
    }
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    nodeGroups->addColumn(transaction, addColumnState);
}

std::pair<offset_t, offset_t> NodeTable::appendToLastNodeGroup(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) const {
    return nodeGroups->appendToLastNodeGroup(transaction, chunkedGroup);
}

void NodeTable::commit(Transaction* transaction, LocalTable* localTable) {
    auto startNodeOffset = nodeGroups->getNumRows();
    transaction->setMaxCommittedNodeOffset(tableID, startNodeOffset);
    auto& localNodeTable = localTable->cast<LocalNodeTable>();
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
    }
    const auto types = getNodeTableColumnTypes(*this);
    const auto dataChunk = constructDataChunk(types);
    ValueVector nodeIDVector(LogicalType::INTERNAL_ID());
    nodeIDVector.setState(dataChunk->state);
    node_group_idx_t nodeGroupToScan = 0u;
    const auto numNodeGroupsToScan = localNodeTable.getNumNodeGroups();
    const auto scanState = std::make_unique<NodeTableScanState>(columnIDs);
    scanState->IDVector = &nodeIDVector;
    for (auto& vector : dataChunk->valueVectors) {
        scanState->outputVectors.push_back(vector.get());
    }
    scanState->source = TableScanSource::UNCOMMITTED;
    while (nodeGroupToScan < numNodeGroupsToScan) {
        // We need to scan from local storage here because some tuples in local node groups might
        // have been deleted.
        scanState->nodeGroup = localNodeTable.getNodeGroup(nodeGroupToScan);
        KU_ASSERT(scanState->nodeGroup);
        scanState->nodeGroup->initializeScanState(transaction, *scanState);
        while (true) {
            auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
            if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
                break;
            }
            for (auto i = 0u; i < scanState->IDVector->state->getSelVector().getSelSize(); i++) {
                const auto pos =
                    scanState->IDVector->state->getSelVector().getSelectedPositions()[i];
                scanState->IDVector->setValue(pos, nodeID_t{startNodeOffset + i, tableID});
            }
            const auto pkVector = scanState->outputVectors[pkColumnID];
            insertPK(transaction, nodeIDVector, *pkVector);
            startNodeOffset += scanState->IDVector->state->getSelVector().getSelSize();
            nodeGroups->append(transaction, scanState->outputVectors);
        }
        nodeGroupToScan++;
    }
    localTable->clear();
}

void NodeTable::insertPK(const Transaction* transaction, const ValueVector& nodeIDVector,
    const ValueVector& pkVector) const {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        const auto nodeIDPos = nodeIDVector.state->getSelVector()[i];
        const auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = pkVector.state->getSelVector()[i];
        if (pkVector.isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(transaction, const_cast<ValueVector*>(&pkVector), pkPos, offset,
                [&](offset_t offset_) { return isVisible(transaction, offset_); })) {
            throw RuntimeException(
                ExceptionMessage::duplicatePKException(pkVector.getAsValue(pkPos)->toString()));
        }
    }
}

void NodeTable::rollback(LocalTable* localTable) {
    localTable->clear();
}

void NodeTable::checkpoint(Serializer& ser) {
    // TODO(Guodong): Should refer to TableCatalogEntry to figure out non-deleted columns.
    std::vector<Column*> checkpointColumns;
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
        checkpointColumns.push_back(columns[i].get());
    }
    NodeGroupCheckpointState state{columnIDs, checkpointColumns, *dataFH, memoryManager};
    nodeGroups->checkpoint(state);
    pkIndex->checkpoint();
    serialize(ser);
    nodeGroups->resetVersionAndUpdateInfo();
}

void NodeTable::serialize(Serializer& serializer) const {
    Table::serialize(serializer);
    nodeGroups->serialize(serializer);
}

uint64_t NodeTable::getEstimatedMemoryUsage() const {
    return nodeGroups->getEstimatedMemoryUsage();
}

bool NodeTable::isVisible(const Transaction* transaction, offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    auto* nodeGroup = getNodeGroup(nodeGroupIdx);
    if (nodeGroup->isDeleted(transaction, offsetInGroup)) {
        return false;
    }
    return nodeGroup->isInserted(transaction, offsetInGroup);
}

bool NodeTable::lookupPK(const Transaction* transaction, ValueVector* keyVector, uint64_t vectorPos,
    offset_t& result) const {
    if (transaction->getLocalStorage()) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        if (localTable &&
            localTable->cast<LocalNodeTable>().lookupPK(transaction, keyVector, result)) {
            return true;
        }
    }
    return pkIndex->lookup(transaction, keyVector, vectorPos, result,
        [&](offset_t offset) { return isVisible(transaction, offset); });
}

} // namespace storage
} // namespace kuzu
