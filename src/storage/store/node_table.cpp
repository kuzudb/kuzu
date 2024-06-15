#include "storage/store/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table_data.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(StorageManager* storageManager, const NodeTableCatalogEntry* nodeTableEntry,
    MemoryManager* memoryManager, VirtualFileSystem* vfs, main::ClientContext* context,
    Deserializer* deSer)
    : Table{nodeTableEntry, storageManager->getNodesStatisticsAndDeletedIDs(), memoryManager,
          &storageManager->getWAL()},
      enableCompression{storageManager->compressionEnabled()}, dataFH{storageManager->getDataFH()},
      metadataDAC{storageManager->getMetadataDAC()},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyPID())} {
    auto& properties = nodeTableEntry->getPropertiesRef();
    const auto maxColumnID =
        std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
            return a.getColumnID() < b.getColumnID();
        })->getColumnID();
    columns.resize(maxColumnID + 1);
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        const auto metadataDAHInfo = dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                                         ->getMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, i);
        const auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[property.getColumnID()] = ColumnFactory::createColumn(columnName,
            *property.getDataType()->copy(), *metadataDAHInfo, dataFH, *metadataDAC, bufferManager,
            wal, &DUMMY_WRITE_TRANSACTION, enableCompression);
    }
    nodeGroups = std::make_unique<NodeGroupCollection>(getTableColumnTypes(*this),
        enableCompression, storageManager->getDataFH(), deSer);
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
}

std::unique_ptr<NodeTable> NodeTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    TableType tableType;
    table_id_t tableID;
    std::string tableName;
    deSer.deserializeValue<TableType>(tableType);
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.deserializeValue<std::string>(tableName);
    // TODO(Guodong): We should move this to table.h once we reowrk RelTable too.
    KU_ASSERT(tableType == TableType::NODE);
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
        nodeTableEntry->getPrimaryKey()->getDataType()->getPhysicalType(), *bufferManager, wal, vfs,
        context);
}

void NodeTable::initializeScanState(Transaction* transaction, TableScanState& scanState) const {
    auto& nodeScanState = scanState.cast<NodeTableScanState>();
    switch (nodeScanState.source) {
    case TableScanSource::COMMITTED: {
        nodeScanState.nodeGroup = &nodeGroups->getNodeGroup(scanState.nodeGroupIdx);
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        auto& localNodeTable = localTable->cast<LocalNodeTable>();
        nodeScanState.nodeGroup = &localNodeTable.getNodeGroup(scanState.nodeGroupIdx);
    } break;
    default: {
        // DO NOTHING.
    }
    }
    nodeScanState.nodeGroup->initializeScanState(transaction, scanState);
}

bool NodeTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    KU_ASSERT(scanState.source != TableScanSource::NONE &&
              scanState.columns.size() == scanState.outputVectors.size());
    for (const auto& outputVector : scanState.outputVectors) {
        (void)outputVector;
        KU_ASSERT(outputVector->state == scanState.nodeIDVector->state);
    }
    return scanState.cast<NodeTableScanState>().nodeGroup->scan(transaction, scanState);
}

void NodeTable::lookup(Transaction* transaction, TableScanState& scanState) const {
    KU_ASSERT(scanState.nodeIDVector->state->getSelVector().getSelSize() == 1);
    scanState.constCast<NodeTableScanState>().nodeGroup->lookup(transaction, scanState);
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
    const auto& nodeInsertState =
        ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(insertState);
    KU_ASSERT(nodeInsertState.propertyVectors[0]->state->getSelVector().getSelSize() == 1);
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
    // TODO(Guodong): Should rework this into first delete the row, then insert it.
    // if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
    // pkIndex->delete_(nodeUpdateState.pkVector);
    // insertPK(nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector);
    // }
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    const auto nodeOffset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_NODES_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->update(updateState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        nodeGroups->getNodeGroup(nodeGroupIdx)
            .update(transaction, nodeOffset, nodeUpdateState.columnID,
                nodeUpdateState.propertyVector);
    }
}

void NodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& nodeDeleteState =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    if (nodeDeleteState.nodeIDVector.isNull(pos)) {
        return;
    }
    const auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    if (nodeOffset >= StorageConstants::MAX_NUM_NODES_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->delete_(transaction, deleteState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        nodeGroups->getNodeGroup(nodeGroupIdx).delete_(transaction, nodeOffset);
    }
}

void NodeTable::addColumn(Transaction* transaction, const Property& property,
    ValueVector* defaultValueVector) {
    const auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    nodesStats->addMetadataDAHInfo(tableID, *property.getDataType());
    const auto colName =
        StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
    const auto metadataDAHInfo =
        nodesStats->getMetadataDAHInfo(transaction, tableID, columns.size());
    auto column = ColumnFactory::createColumn(colName, *property.getDataType()->copy(),
        *metadataDAHInfo, dataFH, *metadataDAC, bufferManager, wal, transaction, enableCompression);
    column->populateWithDefaultVal(transaction, getColumn(pkColumnID).getMetadataDA(),
        defaultValueVector);
    columns.push_back(std::move(column));
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to
    // add tableID into the wal's updated table set separately, as it won't trigger
    // prepareCommit.
    wal->addToUpdatedTables(tableID);
}

std::pair<offset_t, offset_t> NodeTable::appendPartially(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) const {
    return nodeGroups->appendPartially(transaction, chunkedGroup);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    auto startNodeOffset = nodeGroups->getNumRows();
    auto& localNodeTable = localTable->cast<LocalNodeTable>();
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
    }
    std::vector<Column*> columnsToScan;
    for (const auto columnID : columnIDs) {
        columnsToScan.push_back(&getColumn(columnID));
    }
    const auto dataChunkState = std::make_shared<DataChunkState>();
    ValueVector nodeIDVector(*LogicalType::INTERNAL_ID());
    nodeIDVector.setState(dataChunkState);
    std::vector<std::unique_ptr<ValueVector>> vectors;
    for (auto i = 0u; i < columns.size(); i++) {
        vectors.push_back(std::make_unique<ValueVector>(getColumn(i).getDataType(), memoryManager));
    }
    for (auto i = 0u; i < columns.size(); i++) {
        vectors[i]->setState(dataChunkState);
    }
    std::vector<ValueVector*> outputVectors;
    for (auto& vector : vectors) {
        outputVectors.push_back(vector.get());
    }
    node_group_idx_t nodeGroupToScan = 0u;
    const auto numNodeGroupsToScan = localNodeTable.getNumNodeGroups();
    const auto scanState = std::make_unique<NodeTableScanState>(tableID, columnIDs, columnsToScan);
    scanState->nodeIDVector = &nodeIDVector;
    scanState->outputVectors = outputVectors;
    scanState->source = TableScanSource::UNCOMMITTED;
    while (nodeGroupToScan < numNodeGroupsToScan) {
        scanState->nodeGroup = &localNodeTable.getNodeGroup(nodeGroupToScan);
        scanState->nodeGroup->initializeScanState(transaction, *scanState);
        while (scanState->nodeGroup->scan(transaction, *scanState)) {
            const auto numRowsScanned = scanState->nodeIDVector->state->getSelVector().getSelSize();
            for (auto i = 0u; i < numRowsScanned; i++) {
                const auto pos = nodeIDVector.state->getSelVector().getSelectedPositions()[i];
                scanState->nodeIDVector->setValue(pos, nodeID_t{startNodeOffset + i, tableID});
            }
            const auto pkVector = scanState->outputVectors[pkColumnID];
            insertPK(nodeIDVector, *pkVector);
            startNodeOffset += numRowsScanned;
            nodeGroups->append(transaction, scanState->outputVectors);
        }
        nodeGroupToScan++;
    }

    if (pkIndex) {
        pkIndex->prepareCommit();
    }
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommit() {
    pkIndex->prepareCommit();
    for (const auto& column : columns) {
        column->prepareCommit();
    }
}

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
    // Serialize tableType, tableID and name.
    serializer.write<TableType>(tableType);
    serializer.write<table_id_t>(tableID);
    serializer.write<std::string>(tableName);
    // Serialize node groups.
    nodeGroups->serialize(serializer);
}

void NodeTable::checkpointInMemory() {
    for (const auto& column : columns) {
        column->checkpointInMemory();
    }
    pkIndex->checkpointInMemory();
}

void NodeTable::rollbackInMemory() {
    for (const auto& column : columns) {
        column->rollbackInMemory();
    }
    pkIndex->rollbackInMemory();
}

uint64_t NodeTable::getEstimatedMemoryUsage() const {
    return nodeGroups->getEstimatedMemoryUsage();
}

void NodeTable::insertPK(const ValueVector& nodeIDVector, const ValueVector& pkVector) const {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        const auto nodeIDPos = nodeIDVector.state->getSelVector()[i];
        const auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = pkVector.state->getSelVector()[i];
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
