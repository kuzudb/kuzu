#include "storage/store/table_data.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void TableData::insert(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    offset_t lastOffset = nodeIDVector->readNodeOffset(
        nodeIDVector->state->selVector
            ->selectedPositions[nodeIDVector->state->selVector->selectedSize - 1]);
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (lastOffset >= StorageUtils::getStartOffsetOfNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(columns, enableCompression);
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
    KU_ASSERT(columnID < columns.size());
    transaction->getLocalStorage()->update(tableID, columnID, nodeIDVector, propertyVector);
}

void TableData::update(transaction::Transaction* transaction, column_id_t columnID,
    offset_t nodeOffset, ValueVector* propertyVector, sel_t posInPropertyVector) const {
    transaction->getLocalStorage()->update(
        tableID, columnID, nodeOffset, propertyVector, posInPropertyVector);
}

void TableData::addColumn(Transaction* transaction, InMemDiskArray<ColumnChunkMetadata>* metadataDA,
    const MetadataDAHInfo& metadataDAHInfo, const catalog::Property& property,
    ValueVector* defaultValueVector, TablesStatistics* tablesStats) {
    auto column = ColumnFactory::createColumn(*property.getDataType(), metadataDAHInfo, dataFH,
        metadataFH, bufferManager, wal, transaction,
        RWPropertyStats(tablesStats, tableID, property.getPropertyID()), enableCompression);
    column->populateWithDefaultVal(
        property, column.get(), metadataDA, defaultValueVector, getNumNodeGroups(transaction));
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
