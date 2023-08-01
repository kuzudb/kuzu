#include "processor/operator/copy/read_parquet.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadParquet::readTuples(
    std::unique_ptr<ReadFileMorsel> morsel) {
    if (morsel->filePath.empty()) {
        auto serialMorsel = reinterpret_cast<storage::ReadSerialMorsel*>(morsel.get());
        return serialMorsel->recordBatch;
    }
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
    return recordBatch;
}

} // namespace processor
} // namespace kuzu
