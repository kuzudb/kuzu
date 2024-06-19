#include "storage/store/table_data.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/storage_structure/disk_array.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

TableData::TableData(BMFileHandle* dataFH, DiskArrayCollection* metadataDAC,
    catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
    bool enableCompression)
    : dataFH{dataFH}, metadataDAC{metadataDAC}, tableID{tableEntry->getTableID()},
      tableName{tableEntry->getName()}, bufferManager{bufferManager}, wal{wal},
      enableCompression{enableCompression} {}

void TableData::addColumn(Transaction* transaction, const std::string& colNamePrefix,
    DiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDAHInfo,
    const catalog::Property& property, ExpressionEvaluator& defaultEvaluator) {
    auto colName = StorageUtils::getColumnName(property.getName(),
        StorageUtils::ColumnType::DEFAULT, colNamePrefix);
    auto column = ColumnFactory::createColumn(colName, property.getDataType().copy(),
        metadataDAHInfo, dataFH, *metadataDAC, bufferManager, wal, transaction, enableCompression);
    column->populateWithDefaultVal(transaction, metadataDA, defaultEvaluator);
    columns.push_back(std::move(column));
}

void TableData::prepareCommit() {
    for (auto& column : columns) {
        column->prepareCommit();
    }
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
