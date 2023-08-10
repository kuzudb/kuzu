#include "processor/operator/copy_from/read_parquet.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::Table> ReadParquet::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    if (preservingOrder) {
        auto serialMorsel = reinterpret_cast<storage::ReadSerialMorsel*>(morsel.get());
        return serialMorsel->recordTable;
    }
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = TableCopyUtils::createParquetReader(morsel->filePath, sharedState->tableSchema);
        filePath = morsel->filePath;
    }
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        reader->RowGroup(static_cast<int>(morsel->blockIdx))->ReadTable(&table));
    arrow::TableBatchReader batchReader(*table);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    return arrow::Table::FromRecordBatches({recordBatch}).ValueOrDie();
}

} // namespace processor
} // namespace kuzu
