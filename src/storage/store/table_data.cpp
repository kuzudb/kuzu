#include "storage/store/table_data.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void TableData::addColumn(Transaction* transaction, const std::string& colNamePrefix,
    InMemDiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDAHInfo,
    const catalog::Property& property, ValueVector* defaultValueVector,
    TablesStatistics* tablesStats) {
    auto colName = StorageUtils::getColumnName(
        property.getName(), StorageUtils::ColumnType::DEFAULT, colNamePrefix);
    auto column = ColumnFactory::createColumn(colName, *property.getDataType()->copy(),
        metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, transaction,
        RWPropertyStats(tablesStats, tableID, property.getPropertyID()), enableCompression);
    column->populateWithDefaultVal(transaction, metadataDA, defaultValueVector);
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
