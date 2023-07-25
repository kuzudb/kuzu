#include "processor/operator/copy/read_parquet.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadParquet::readTuples(
    std::unique_ptr<ReadFileMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = storage::TableCopyUtils::createParquetReader(
            morsel->filePath, sharedState->tableSchema);
        filePath = morsel->filePath;
    }
    std::shared_ptr<arrow::Table> table;
    storage::TableCopyUtils::throwCopyExceptionIfNotOK(
        reader->RowGroup(static_cast<int>(morsel->blockIdx))->ReadTable(&table));
    arrow::TableBatchReader batchReader(*table);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    storage::TableCopyUtils::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    return recordBatch;
}

} // namespace processor
} // namespace kuzu
