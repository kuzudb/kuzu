#include "storage/store/table_data.h"

#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

TableData::TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    BufferManager* bufferManager, WAL* wal, bool enableCompression, ColumnDataFormat dataFormat)
    : dataFH{dataFH}, metadataFH{metadataFH}, tableID{tableID}, bufferManager{bufferManager},
      wal{wal}, enableCompression{enableCompression}, dataFormat{dataFormat} {}

void TableData::read(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (nodeIDVector->isSequential()) {
        scan(transaction, nodeIDVector, columnIDs, outputVectors);
    } else {
        lookup(transaction, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::scan(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIDs.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->scan(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::lookup(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->lookup(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::insert(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    offset_t lastOffset = nodeIDVector->readNodeOffset(
        nodeIDVector->state->selVector
            ->selectedPositions[nodeIDVector->state->selVector->selectedSize - 1]);
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (lastOffset >= StorageUtils::getStartOffsetOfNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(this);
        newNodeGroup->finalize(currentNumNodeGroups);
        append(newNodeGroup.get());
    }
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        if (columns[columnID]->getDataType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            continue;
        }
        transaction->getLocalStorage()->update(
            tableID, columnID, nodeIDVector, propertyVectors[columnID]);
    }
}

void TableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* nodeIDVector, ValueVector* propertyVector) {
    assert(columnID < columns.size());
    transaction->getLocalStorage()->update(tableID, columnID, nodeIDVector, propertyVector);
}

void TableData::update(transaction::Transaction* transaction, column_id_t columnID,
    offset_t nodeOffset, ValueVector* propertyVector, sel_t posInPropertyVector) const {
    transaction->getLocalStorage()->update(
        tableID, columnID, nodeOffset, propertyVector, posInPropertyVector);
}

void TableData::append(kuzu::storage::NodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto columnChunk = nodeGroup->getColumnChunk(columnID);
        assert(columnID < columns.size());
        columns[columnID]->append(columnChunk, nodeGroup->getNodeGroupIdx());
    }
}

void TableData::addColumn(transaction::Transaction* transaction, const catalog::Property& property,
    ValueVector* defaultValueVector, TablesStatistics* tablesStats) {
    auto metadataDAHInfo = dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStats)
                               ->getMetadataDAHInfo(transaction, tableID, columns.size());
    auto column = ColumnFactory::createColumn(*property.getDataType(), *metadataDAHInfo, dataFH,
        metadataFH, bufferManager, wal, transaction,
        RWPropertyStats(tablesStats, tableID, property.getPropertyID()), enableCompression);
    column->populateWithDefaultVal(
        property, column.get(), defaultValueVector, getNumNodeGroups(transaction));
    columns.push_back(std::move(column));
}

void TableData::checkpointInMemory() {
    for (auto& column : columns) {
        column->checkpointInMemory();
    }
}

void TableData::rollbackInMemory() {
    for (auto& column : columns) {
        column->rollbackInMemory();
    }
}

} // namespace storage
} // namespace kuzu
