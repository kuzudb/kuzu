#include "storage/store/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/local_storage/local_node_table.h"
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
            property.getDataType().copy(), dataFH, bufferManager, wal, enableCompression);
    }
    nodeGroups = std::make_unique<NodeGroupCollection>(getNodeTableColumnTypes(*this),
        enableCompression, 0 /*startNodeOffset*/, storageManager->getDataFH(), deSer);
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
}

std::unique_ptr<NodeTable> NodeTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    table_id_t tableID;
    std::string tableName;
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.deserializeValue<std::string>(tableName);
    auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID)
                            ->ptrCast<NodeTableCatalogEntry>();
    KU_ASSERT(catalogEntry->getName() == tableName);
    return std::make_unique<NodeTable>(storageManager, catalogEntry, memoryManager, vfs, context,
        &deSer);
}

void NodeTable::initializePKIndex(const std::string& databasePath,
    const NodeTableCatalogEntry* nodeTableEntry, bool readOnly, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    pkIndex = std::make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(vfs, databasePath, tableID), readOnly,
        nodeTableEntry->getPrimaryKey()->getDataType().getPhysicalType(), *bufferManager, wal, vfs,
        context);
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
    const auto rowIdxInGroup =
        nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    scanState.rowIdxVector->setValue<row_idx_t>(nodeIDPos, rowIdxInGroup);
    return scanState.nodeGroup->lookup(transaction, scanState);
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
    const auto& nodeInsertState = insertState.cast<NodeTableInsertState>();
    KU_ASSERT(nodeInsertState.propertyVectors[0]->state->getSelVector().getSelSize() == 1);
    KU_ASSERT(nodeInsertState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    if (nodeInsertState.nodeIDVector.isNull(
            nodeInsertState.nodeIDVector.state->getSelVector()[0])) {
        return;
    }
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(transaction, insertState);
}

void NodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    const auto& nodeUpdateState =
        ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(updateState);
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    if (nodeUpdateState.nodeIDVector.isNull(pos)) {
        return;
    }
    if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
        insertPK(nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector);
    }
    const auto nodeOffset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->update(updateState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups->getNodeGroup(nodeGroupIdx)
            ->update(transaction, rowIdxInGroup, nodeUpdateState.columnID,
                nodeUpdateState.propertyVector);
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
    const auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        return localTable->delete_(transaction, deleteState);
    }
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    const auto rowIdxInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    return nodeGroups->getNodeGroup(nodeGroupIdx)->delete_(transaction, rowIdxInGroup);
}

// TODO(FIX-ME): Rework this.
void NodeTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    auto& property = addColumnState.property;
    KU_ASSERT(property.getColumnID() == columns.size());
    columns.push_back(ColumnFactory::createColumn(property.getName(), property.getDataType().copy(),
        dataFH, bufferManager, wal, enableCompression));
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    nodeGroups->addColumn(transaction, addColumnState);
}

std::pair<offset_t, offset_t> NodeTable::appendToLastNodeGroup(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) const {
    return nodeGroups->appendToLastNodeGroup(transaction, chunkedGroup);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    auto startNodeOffset = nodeGroups->getNumRows();
    transaction->setMaxCommittedNodeOffset(tableID, startNodeOffset);
    auto& localNodeTable = localTable->cast<LocalNodeTable>();
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
    }
    auto types = getNodeTableColumnTypes(*this);
    const auto dataChunk = constructDataChunk(types);
    ValueVector nodeIDVector(LogicalType::INTERNAL_ID());
    nodeIDVector.setState(dataChunk->state);
    node_group_idx_t nodeGroupToScan = 0u;
    const auto numNodeGroupsToScan = localNodeTable.getNumNodeGroups();
    const auto scanState = std::make_unique<NodeTableScanState>(tableID, columnIDs);
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

void NodeTable::prepareCommit() {
    pkIndex->prepareCommit();
}

// TODO: Remove this.
void NodeTable::prepareRollback(LocalTable* localTable) {
    pkIndex->prepareRollback();
    localTable->clear();
}

void NodeTable::checkpoint(Serializer& ser) {
    std::vector<Column*> checkpointColumns;
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
        checkpointColumns.push_back(columns[i].get());
    }
    const NodeGroupCheckpointState state{columnIDs, checkpointColumns, *dataFH};
    nodeGroups->checkpoint(state);
    serialize(ser);
    checkpointInMemory();
}

void NodeTable::serialize(Serializer& serializer) const {
    Table::serialize(serializer);
    nodeGroups->serialize(serializer);
}

void NodeTable::checkpointInMemory() {
    pkIndex->checkpointInMemory();
}

void NodeTable::rollbackInMemory() {
    pkIndex->rollbackInMemory();
}

uint64_t NodeTable::getEstimatedMemoryUsage() const {
    return nodeGroups->getEstimatedMemoryUsage();
}

void NodeTable::insertPK(Transaction* transaction, const ValueVector& nodeIDVector,
    const ValueVector& pkVector) const {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        const auto nodeIDPos = nodeIDVector.state->getSelVector()[i];
        const auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = pkVector.state->getSelVector()[i];
        if (pkVector.isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(transaction, const_cast<ValueVector*>(&pkVector), pkPos, offset)) {
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
