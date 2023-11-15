#include "storage/store/node_table_data.h"

#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTableData::NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    BufferManager* bufferManager, WAL* wal, const std::vector<Property*>& properties,
    TablesStatistics* tablesStatistics, bool enableCompression)
    : TableData{dataFH, metadataFH, tableID, bufferManager, wal, enableCompression,
          ColumnDataFormat::REGULAR} {
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = properties[i];
        auto metadataDAHInfo = dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                                   ->getMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, i);
        columns.push_back(ColumnFactory::createColumn(property->getDataType()->copy(),
            *metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
            RWPropertyStats(tablesStatistics, tableID, property->getPropertyID()),
            enableCompression));
    }
}

void NodeTableData::scan(Transaction* transaction, TableReadState& readState,
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(readState.columnIDs.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        if (readState.columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            KU_ASSERT(readState.columnIDs[i] < columns.size());
            columns[readState.columnIDs[i]]->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->scan(
            tableID, nodeIDVector, readState.columnIDs, outputVectors);
    }
}

void NodeTableData::insert(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    offset_t lastOffset =
        nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (lastOffset >= StorageUtils::getStartOffsetOfNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(columns, enableCompression);
        newNodeGroup->finalize(currentNumNodeGroups);
        append(newNodeGroup.get());
    }
    auto localStorage = transaction->getLocalStorage();
    localStorage->initializeLocalTable(tableID, columns);
    localStorage->insert(tableID, nodeIDVector, propertyVectors);
}

void NodeTableData::update(Transaction* transaction, column_id_t columnID,
    ValueVector* nodeIDVector, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size());
    auto localStorage = transaction->getLocalStorage();
    localStorage->initializeLocalTable(tableID, columns);
    localStorage->update(tableID, nodeIDVector, columnID, propertyVector);
}

void NodeTableData::delete_(Transaction* transaction, ValueVector* nodeIDVector) {
    auto localStorage = transaction->getLocalStorage();
    localStorage->initializeLocalTable(tableID, columns);
    localStorage->delete_(tableID, nodeIDVector);
}

void NodeTableData::lookup(Transaction* transaction, TableReadState& readState,
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            KU_ASSERT(readState.columnIDs[i] < columns.size());
            columns[readState.columnIDs[i]]->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->lookup(
            tableID, nodeIDVector, readState.columnIDs, outputVectors);
    }
}

void NodeTableData::append(kuzu::storage::NodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto columnChunk = nodeGroup->getColumnChunk(columnID);
        KU_ASSERT(columnID < columns.size());
        columns[columnID]->append(columnChunk, nodeGroup->getNodeGroupIdx());
    }
}

void NodeTableData::prepareLocalTableToCommit(LocalTable* localTable) {
    auto numNodeGroups = getNumNodeGroups(&DUMMY_WRITE_TRANSACTION);
    for (auto& [nodeGroupIdx, nodeGroup] : localTable->nodeGroups) {
        for (auto columnID = 0; columnID < columns.size(); columnID++) {
            auto column = columns[columnID].get();
            auto columnChunk = nodeGroup->getLocalColumnChunk(columnID);
            if (columnChunk->getNumRows() == 0) {
                continue;
            }
            column->prepareCommitForChunk(nodeGroupIdx, columnChunk, nodeGroupIdx >= numNodeGroups);
        }
    }
}

} // namespace storage
} // namespace kuzu
