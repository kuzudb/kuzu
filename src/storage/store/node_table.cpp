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
    MemoryManager* memoryManager, VirtualFileSystem* vfs, main::ClientContext* context)
    : Table{nodeTableEntry, storageManager->getNodesStatisticsAndDeletedIDs(), memoryManager,
          &storageManager->getWAL()},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyPID())} {
    tableData = std::make_unique<NodeTableData>(storageManager->getDataFH(),
        storageManager->getMetadataDAC(), nodeTableEntry, bufferManager, wal,
        nodeTableEntry->getPropertiesRef(), storageManager->getNodesStatisticsAndDeletedIDs(),
        storageManager->compressionEnabled());
    deltaNodeGroups = std::make_unique<NodeGroupCollection>(getTableColumnTypes(*this),
        storageManager->getDataFH(), *tableData);
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
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
    switch (auto& nodeScanState = scanState.cast<NodeTableScanState>(); nodeScanState.source) {
    case TableScanSource::COMMITTED: {
        nodeScanState.nodeGroup = &deltaNodeGroups->getNodeGroup(scanState.nodeGroupIdx);
        nodeScanState.nodeGroup->initializeScanState(transaction, scanState);
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        auto& localNodeTable = localTable->cast<LocalNodeTable>();
        nodeScanState.nodeGroup = &localNodeTable.getNodeGroup(scanState.nodeGroupIdx);
        nodeScanState.nodeGroup->initializeScanState(transaction, scanState);
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
    // TODO: Eventually the logic should be like this:
    //       - Check node groups had been cheeckpointed first to find deleted offsets.
    //       - If there is no deleted offset, then insert the node.
    //       - If there is a deleted offset, then reuse the offset, and turn this into an update.
    //       Turned off the reusing of deleted offsets for now.
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
    if (nodeUpdateState.columnID == pkColumnID) {
        pkIndex->delete_(nodeUpdateState.pkVector);
        insertPK(nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector);
    }
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->update(updateState);
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
    if (nodeOffset > StorageConstants::MAX_NUM_NODES_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->delete_(transaction, deleteState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto actualDeleted =
            deltaNodeGroups->getNodeGroup(nodeGroupIdx).delete_(transaction, nodeOffset);
        if (actualDeleted) {
            // TODO: Add record to undo buffer.
        }
    }
}

void NodeTable::addColumn(Transaction* transaction, const Property& property,
    ValueVector* defaultValueVector) {
    const auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    nodesStats->addMetadataDAHInfo(tableID, *property.getDataType());
    tableData->addColumn(transaction, "", tableData->getColumn(pkColumnID)->getMetadataDA(),
        *nodesStats->getMetadataDAHInfo(transaction, tableID, tableData->getNumColumns()), property,
        defaultValueVector);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

offset_t NodeTable::append(Transaction* transaction, ChunkedNodeGroup* chunkedGroup,
    offset_t startOffsetToAppend, row_idx_t numRowsToAppend) {
    // TODO: handle startOffsetToAppend and numRowsToAppend.
    deltaNodeGroups->merge(transaction, chunkedGroup->getNodeGroupIdx(), *chunkedGroup);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    std::unique_lock xLck{mtx};
    auto& localNodeTable = localTable->cast<LocalNodeTable>();
    // 1. Grab a set of node offsets from table statistics for local insertions.
    const auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    const auto numNodesCheckpointed = nodesStats->getNumTuplesForTable(transaction, tableID);
    auto startNodeOffset = numNodesCheckpointed + deltaNodeGroups->getNumRows();

    // 2. Scan local table to populate hash index.
    std::vector<column_id_t> columnsToScan{pkColumnID};
    const auto dataChunkState = std::make_shared<DataChunkState>();
    ValueVector nodeIDVector(*LogicalType::INTERNAL_ID());
    nodeIDVector.setState(dataChunkState);
    ValueVector pkVector(tableData->getColumn(pkColumnID)->getDataType());
    pkVector.setState(dataChunkState);
    std::vector<ValueVector*> outputVectors{&pkVector};
    node_group_idx_t nodeGroupToScan = 0u;
    auto numNodeGroupsToScan = localNodeTable.getNumNodeGroups();
    const auto scanState =
        std::make_unique<NodeTableScanState>(tableID, &nodeIDVector, columnsToScan, outputVectors);
    scanState->source = TableScanSource::UNCOMMITTED;
    while (nodeGroupToScan < numNodeGroupsToScan) {
        scanState->nodeGroup = &localNodeTable.getNodeGroup(nodeGroupToScan);
        scanState->nodeGroup->initializeScanState(transaction, *scanState);
        while (scanState->nodeGroup->scan(transaction, *scanState)) {
            const auto numRowsScanned = scanState->nodeIDVector->state->getSelVector().getSelSize();
            for (auto i = 0u; i < numRowsScanned; i++) {
                scanState->nodeIDVector->setValue(i, nodeID_t{startNodeOffset + i, tableID});
            }
            insertPK(nodeIDVector, pkVector);
            startNodeOffset += numRowsScanned;
        }
        nodeGroupToScan++;
    }

    // 3. Append the node groups to the table data.
    deltaNodeGroups->append(transaction, localNodeTable.getNodeGroups());

    // 4. TODO: Add record to undo buffer.

    if (pkIndex) {
        pkIndex->prepareCommit();
    }
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

void NodeTable::checkpoint() {
    // 1. Flush the delta node groups to disk.
    deltaNodeGroups->checkpoint();
    // 2. Update metadata disk arrays.
    const auto numNodeGroups = deltaNodeGroups->getNumNodeGroups();
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        auto& nodeGroup = deltaNodeGroups->getNodeGroup(nodeGroupIdx);
        KU_ASSERT(
            nodeGroup.getNumChunkedGroups() == 1 && nodeGroup.getType() == NodeGroupType::ON_DISK);
        auto& chunkedGroup = nodeGroup.getChunkedGroup(0);
        for (auto columnID = 0u; columnID < chunkedGroup.getNumColumns(); columnID++) {
            tableData->getColumn(columnID)->setMetadataFromChunk(nodeGroupIdx,
                chunkedGroup.getColumnChunk(columnID));
        }
    }
    // 3. Should also update table statistics.
    tablesStatistics->updateNumTuplesByValue(tableID, deltaNodeGroups->getNumRows());
    checkpointInMemory();
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
