#include "processor/operator/copy/read_parquet.h"

namespace kuzu {
namespace processor {

void ReadParquetSharedState::countNumLines() {
    for (auto& filePath : filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            storage::TableCopyUtils::createParquetReader(filePath);
        auto metadata = reader->parquet_reader()->metadata();
        uint64_t numBlocks = metadata->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        auto startNodeOffset = numRows;
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            numLinesPerBlock[blockIdx] = metadata->RowGroup(blockIdx)->num_rows();
        }
        fileBlockInfos.emplace(
            filePath, storage::FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
        numRows += metadata->num_rows();
    }
}

std::unique_ptr<ReadFileMorsel> ReadParquetSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (curFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[curFileIdx];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (curBlockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read in this file.
            curFileIdx++;
            curBlockIdx = 0;
            continue;
        }
        auto result = std::make_unique<ReadFileMorsel>(
            nodeOffset, curBlockIdx, fileBlockInfo.numLinesPerBlock[curBlockIdx], filePath);
        nodeOffset += fileBlockInfos.at(filePath).numLinesPerBlock[curBlockIdx];
        curBlockIdx++;
        return result;
    }
}

std::shared_ptr<arrow::RecordBatch> ReadParquet::readTuples(
    std::unique_ptr<ReadFileMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = storage::TableCopyUtils::createParquetReader(morsel->filePath);
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
