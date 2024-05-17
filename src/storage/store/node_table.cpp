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

NodeTable::NodeTable(StorageManager* storageManager, NodeTableCatalogEntry* nodeTableEntry,
    MemoryManager* memoryManager, VirtualFileSystem* vfs)
    : Table{nodeTableEntry, storageManager->getNodesStatisticsAndDeletedIDs(), memoryManager,
          &storageManager->getWAL()},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyPID())} {
    tableData = std::make_unique<NodeTableData>(storageManager->getDataFH(),
        storageManager->getMetadataFH(), nodeTableEntry, bufferManager, wal,
        nodeTableEntry->getPropertiesRef(), storageManager->getNodesStatisticsAndDeletedIDs(),
        storageManager->compressionEnabled());
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs);
}

void NodeTable::initializePKIndex(const std::string& databasePath,
    NodeTableCatalogEntry* nodeTableEntry, bool readOnly, VirtualFileSystem* vfs) {
    if (nodeTableEntry->getPrimaryKey()->getDataType()->getLogicalTypeID() !=
        LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(vfs, databasePath, tableID), readOnly,
            nodeTableEntry->getPrimaryKey()->getDataType()->getPhysicalType(), *bufferManager, wal,
            vfs);
    }
}

void NodeTable::readInternal(Transaction* transaction, TableReadState& readState) {
    tableData->read(transaction, *readState.dataReadState, *readState.nodeIDVector,
        readState.outputVectors);
    auto& dataReadState = ku_dynamic_cast<const TableDataReadState&, const NodeDataReadState&>(
        *readState.dataReadState);
    if (dataReadState.localNodeGroup) {
        KU_ASSERT(transaction->isWriteTransaction());
        dataReadState.localNodeGroup->scan(*readState.nodeIDVector, readState.columnIDs,
            readState.outputVectors);
    }
}

offset_t NodeTable::validateUniquenessConstraint(Transaction* tx,
    const std::vector<ValueVector*>& propertyVectors) {
    if (pkIndex == nullptr) {
        return INVALID_OFFSET;
    }
    auto pkVector = propertyVectors[pkColumnID];
    KU_ASSERT(pkVector->state->getSelVector().getSelSize() == 1);
    auto pkVectorPos = pkVector->state->getSelVector()[0];
    offset_t offset;
    if (pkIndex->lookup(tx, propertyVectors[pkColumnID], pkVectorPos, offset)) {
        return offset;
    }
    return INVALID_OFFSET;
}

void NodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    auto& nodeInsertState = ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(insertState);
    KU_ASSERT(nodeInsertState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    auto pos = nodeInsertState.nodeIDVector.state->getSelVector()[0];
    auto offset = nodesStats->addNode(tableID);
    nodeInsertState.nodeIDVector.setValue(pos, nodeID_t{offset, tableID});
    nodeInsertState.nodeIDVector.setNull(pos, false);
    if (pkIndex) {
        insertPK(nodeInsertState.nodeIDVector, nodeInsertState.pkVector);
    }
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(insertState);
}

void NodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    auto& nodeUpdateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(updateState);
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
        updatePK(transaction, updateState.columnID, nodeUpdateState.nodeIDVector,
            updateState.propertyVector);
    }
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->update(updateState);
}

void NodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    auto& nodeDeleteState = ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    if (nodeDeleteState.nodeIDVector.isNull(pos)) {
        return;
    }
    if (pkIndex) {
        pkIndex->delete_(&nodeDeleteState.pkVector);
    }
    auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
        ->deleteNode(tableID, nodeOffset);
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->delete_(deleteState);
}

void NodeTable::addColumn(Transaction* transaction, const Property& property,
    ValueVector* defaultValueVector) {
    auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    nodesStats->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics(!defaultValueVector->hasNoNullsGuarantee()));
    nodesStats->addMetadataDAHInfo(tableID, *property.getDataType());
    tableData->addColumn(transaction, "", tableData->getColumn(pkColumnID)->getMetadataDA(),
        *nodesStats->getMetadataDAHInfo(transaction, tableID, tableData->getNumColumns()), property,
        defaultValueVector, nodesStats);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommitNodeGroup(node_group_idx_t nodeGroupIdx,
    transaction::Transaction* transaction, LocalNodeNG* localNodeGroup) {
    tableData->prepareLocalNodeGroupToCommit(nodeGroupIdx, transaction, localNodeGroup);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    if (pkIndex) {
        pkIndex->prepareCommit();
    }
    auto localNodeTable = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable);
    tableData->prepareLocalTableToCommit(transaction, localNodeTable->getTableData());
    tableData->prepareCommit();
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommit() {
    if (pkIndex) {
        pkIndex->prepareCommit();
    }
    tableData->prepareCommit();
}

void NodeTable::prepareRollback(LocalTable* localTable) {
    if (pkIndex) {
        pkIndex->prepareRollback();
    }
    localTable->clear();
}

void NodeTable::checkpointInMemory() {
    tableData->checkpointInMemory();
    if (pkIndex) {
        pkIndex->checkpointInMemory();
    }
}

void NodeTable::rollbackInMemory() {
    tableData->rollbackInMemory();
    if (pkIndex) {
        pkIndex->rollbackInMemory();
    }
}

void NodeTable::updatePK(Transaction* transaction, column_id_t columnID,
    const ValueVector& nodeIDVector, const ValueVector& payloadVector) {
    auto pkVector =
        std::make_unique<ValueVector>(getColumn(pkColumnID)->getDataType(), memoryManager);
    pkVector->state = nodeIDVector.state;
    auto outputVectors = std::vector<ValueVector*>{pkVector.get()};
    auto columnIDs = {columnID};
    auto readState = std::make_unique<NodeTableReadState>(&nodeIDVector, columnIDs, outputVectors);
    initializeReadState(transaction, columnIDs, *readState);
    read(transaction, *readState);
    pkIndex->delete_(pkVector.get());
    insertPK(nodeIDVector, payloadVector);
}

void NodeTable::insertPK(const ValueVector& nodeIDVector, const ValueVector& primaryKeyVector) {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        auto nodeIDPos = nodeIDVector.state->getSelVector()[0];
        auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = primaryKeyVector.state->getSelVector()[0];
        if (primaryKeyVector.isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(const_cast<ValueVector*>(&primaryKeyVector), pkPos, offset)) {
            std::string pkStr;
            TypeUtils::visit(
                primaryKeyVector.dataType.getPhysicalType(),
                [&](ku_string_t) {
                    pkStr = primaryKeyVector.getValue<ku_string_t>(pkPos).getAsString();
                },
                [&pkStr, &primaryKeyVector, &pkPos]<typename T>(
                    T) { pkStr = TypeUtils::toString(primaryKeyVector.getValue<T>(pkPos)); });
            throw RuntimeException(ExceptionMessage::duplicatePKException(pkStr));
        }
    }
}

} // namespace storage
} // namespace kuzu
