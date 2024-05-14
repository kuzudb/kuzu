#include "storage/store/node_table_data.h"

#include "common/cast.h"
#include "common/types/types.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/stats/nodes_store_statistics.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTableData::NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
    const std::vector<Property>& properties, TablesStatistics* tablesStatistics,
    bool enableCompression)
    : TableData{dataFH, metadataFH, tableEntry, bufferManager, wal, enableCompression} {
    auto maxColumnID = std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
        return a.getColumnID() < b.getColumnID();
    })->getColumnID();
    columns.resize(maxColumnID + 1);
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        auto metadataDAHInfo = dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                                   ->getMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, i);
        auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[property.getColumnID()] =
            ColumnFactory::createColumn(columnName, *property.getDataType()->copy(),
                *metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
                RWPropertyStats(tablesStatistics, tableID, property.getPropertyID()),
                enableCompression);
    }
}

void NodeTableData::initializeReadState(Transaction* transaction,
    std::vector<column_id_t> columnIDs, const ValueVector& inNodeIDVector,
    TableDataReadState& readState) {
    readState.columnIDs = columnIDs;
    auto& dataReadState = ku_dynamic_cast<TableDataReadState&, NodeDataReadState&>(readState);
    if (dataReadState.nodeGroupIdx == INVALID_NODE_GROUP_IDX) {
        dataReadState.columnReadStates.resize(columnIDs.size());
    }
    KU_ASSERT(dataReadState.columnReadStates.size() == columnIDs.size());
    auto startNodeOffset = INVALID_OFFSET;
    if (inNodeIDVector.isSequential()) {
        startNodeOffset = inNodeIDVector.readNodeOffset(0);
        KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    } else {
        auto pos = inNodeIDVector.state->getSelVector()[0];
        startNodeOffset = inNodeIDVector.readNodeOffset(pos);
    }
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    // Sanity check on that all nodeIDs in the input vector are from the same node group.
    for (auto i = 0u; i < inNodeIDVector.state->getSelVector().getSelSize(); i++) {
        KU_ASSERT(StorageUtils::getNodeGroupIdx(inNodeIDVector.readNodeOffset(
                      inNodeIDVector.state->getSelVector()[i])) == nodeGroupIdx);
    }
    auto numExistingNodeGroups = columns[0]->getNumNodeGroups(transaction);
    // Sanity check on that all columns should have the same num of node groups.
    for (auto i = 1u; i < columns.size(); i++) {
        KU_ASSERT(columns[i]->getNumNodeGroups(transaction) == numExistingNodeGroups);
    }
    dataReadState.readFromPersistent = nodeGroupIdx < numExistingNodeGroups;
    // Sanity check on that we should always be able to readFromPersistent if the transaction is
    // read only.
    KU_ASSERT((dataReadState.readFromPersistent && transaction->isReadOnly()) ||
              transaction->isWriteTransaction());
    if (dataReadState.readFromPersistent) {
        initializeColumnReadStates(transaction, dataReadState, nodeGroupIdx);
    }
    if (transaction->isWriteTransaction()) {
        initializeLocalNodeReadState(transaction, dataReadState, nodeGroupIdx);
    }
    dataReadState.nodeGroupIdx = nodeGroupIdx;
}

void NodeTableData::initializeColumnReadStates(Transaction* transaction,
    NodeDataReadState& readState, node_group_idx_t nodeGroupIdx) {
    auto& dataReadState = ku_dynamic_cast<TableDataReadState&, NodeDataReadState&>(readState);
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        if (readState.columnIDs[i] != INVALID_COLUMN_ID) {
            getColumn(readState.columnIDs[i])
                ->initChunkState(transaction, nodeGroupIdx, dataReadState.columnReadStates[i]);
        }
    }
    if (nodeGroupIdx != dataReadState.nodeGroupIdx) {
        KU_ASSERT(sanityCheckOnColumnNumValues(dataReadState));
    }
}

bool NodeTableData::sanityCheckOnColumnNumValues(
    const kuzu::storage::NodeDataReadState& readState) {
    // Sanity check on that all valid columns should have the same numValues in the node
    // group.
    auto validColumn = std::find_if(readState.columnIDs.begin(), readState.columnIDs.end(),
        [](column_id_t columnID) { return columnID != INVALID_COLUMN_ID; });
    if (validColumn != readState.columnIDs.end()) {
        auto numValues = readState.columnReadStates[validColumn - readState.columnIDs.begin()]
                             .metadata.numValues;
        for (auto i = 0u; i < readState.columnIDs.size(); i++) {
            if (readState.columnIDs[i] == INVALID_COLUMN_ID) {
                continue;
            }
            if (readState.columnReadStates[i].metadata.numValues != numValues) {
                KU_ASSERT(false);
                return false;
            }
        }
    }
    return true;
}

void NodeTableData::initializeLocalNodeReadState(transaction::Transaction* transaction,
    kuzu::storage::NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx) {
    if (readState.nodeGroupIdx == nodeGroupIdx) {
        return;
    }
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    readState.localNodeGroup = nullptr;
    if (localTable) {
        auto localNodeTable = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable);
        if (localNodeTable->getTableData()->nodeGroups.contains(nodeGroupIdx)) {
            readState.localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(
                localNodeTable->getTableData()->nodeGroups.at(nodeGroupIdx).get());
        }
    }
}

void NodeTableData::read(Transaction* transaction, TableDataReadState& readState,
    const ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto& nodeReadState = ku_dynamic_cast<TableDataReadState&, NodeDataReadState&>(readState);
    if (!nodeReadState.readFromPersistent) {
        return;
    }
    if (nodeIDVector.isSequential()) {
        scan(transaction, readState, nodeIDVector, outputVectors);
    } else {
        lookup(transaction, readState, nodeIDVector, outputVectors);
    }
}

void NodeTableData::scan(Transaction* transaction, TableDataReadState& readState,
    const ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(readState.columnIDs.size() == outputVectors.size() && !nodeIDVector.state->isFlat());
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        if (readState.columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            KU_ASSERT(readState.columnIDs[i] < columns.size());
            auto& nodeDataReadState =
                ku_dynamic_cast<TableDataReadState&, NodeDataReadState&>(readState);
            // TODO: Remove `const_cast` on nodeIDVector.
            columns[readState.columnIDs[i]]->scan(transaction,
                nodeDataReadState.columnReadStates[i], const_cast<ValueVector*>(&nodeIDVector),
                outputVectors[i]);
        }
    }
}

void NodeTableData::lookup(Transaction* transaction, TableDataReadState& readState,
    const ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    for (auto columnIdx = 0u; columnIdx < readState.columnIDs.size(); columnIdx++) {
        auto columnID = readState.columnIDs[columnIdx];
        if (columnID == INVALID_COLUMN_ID) {
            KU_ASSERT(outputVectors[columnIdx]->state == nodeIDVector.state);
            for (auto i = 0u; i < outputVectors[columnIdx]->state->getSelVector().getSelSize();
                 i++) {
                auto pos = outputVectors[columnIdx]->state->getSelVector()[i];
                outputVectors[i]->setNull(pos, true);
            }
        } else {
            KU_ASSERT(readState.columnIDs[columnIdx] < columns.size());
            auto& nodeDataReadState =
                ku_dynamic_cast<TableDataReadState&, NodeDataReadState&>(readState);
            // TODO: Remove `const_cast` on nodeIDVector.
            columns[readState.columnIDs[columnIdx]]->lookup(transaction,
                nodeDataReadState.columnReadStates[columnIdx],
                const_cast<ValueVector*>(&nodeIDVector), outputVectors[columnIdx]);
        }
    }
}

void NodeTableData::append(ChunkedNodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto& columnChunk = nodeGroup->getColumnChunkUnsafe(columnID);
        KU_ASSERT(columnID < columns.size());
        columns[columnID]->append(&columnChunk, nodeGroup->getNodeGroupIdx());
    }
}

void NodeTableData::prepareLocalNodeGroupToCommit(node_group_idx_t nodeGroupIdx,
    Transaction* transaction, LocalNodeNG* localNodeGroup) {
    auto numNodeGroups = columns[0]->getNumNodeGroups(transaction);
    auto isNewNodeGroup = nodeGroupIdx >= numNodeGroups;
    KU_ASSERT(std::find_if(columns.begin(), columns.end(), [&](const auto& column) {
        return column->getNumNodeGroups(transaction) != numNodeGroups;
    }) == columns.end());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto column = columns[columnID].get();
        auto localInsertChunk = localNodeGroup->getInsertChunks().getLocalChunk(columnID);
        auto localUpdateChunk = localNodeGroup->getUpdateChunks(columnID).getLocalChunk(0);
        if (localInsertChunk.empty() && localUpdateChunk.empty()) {
            continue;
        }
        column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunk,
            localNodeGroup->getInsertInfoRef(), localUpdateChunk,
            localNodeGroup->getUpdateInfoRef(columnID), {} /* deleteInfo */);
    }
}

void NodeTableData::prepareLocalTableToCommit(Transaction* transaction,
    LocalTableData* localTable) {
    for (auto& [nodeGroupIdx, localNodeGroup] : localTable->nodeGroups) {
        prepareLocalNodeGroupToCommit(nodeGroupIdx, transaction,
            ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(localNodeGroup.get()));
    }
}

} // namespace storage
} // namespace kuzu
