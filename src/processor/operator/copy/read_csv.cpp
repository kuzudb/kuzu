#include "processor/operator/copy/read_csv.h"

namespace kuzu {
namespace processor {

void ReadCSVSharedState::countNumRows() {
    for (auto& filePath : filePaths) {
        auto csvStreamingReader =
            storage::TableCopyUtils::createCSVReader(filePath, &csvReaderConfig, tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numRowsPerBlock;
        auto startRowIdx = numRows;
        while (true) {
            storage::TableCopyUtils::throwCopyExceptionIfNotOK(
                csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == nullptr) {
                break;
            }
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numRowsPerBlock.push_back(currNumRows);
            numRows += currNumRows;
        }
        fileBlockInfos.emplace(
            filePath, storage::FileBlockInfo{startRowIdx, numBlocks, numRowsPerBlock});
    }
}

std::unique_ptr<ReadFileMorsel> ReadCSVSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (curFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[curFileIdx];
        if (!reader) {
            reader =
                storage::TableCopyUtils::createCSVReader(filePath, &csvReaderConfig, tableSchema);
        }
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        storage::TableCopyUtils::throwCopyExceptionIfNotOK(reader->ReadNext(&recordBatch));
        if (recordBatch == nullptr) {
            // No more blocks to read in this file.
            curFileIdx++;
            currRowIdxInCurrFile = 1;
            reader.reset();
            continue;
        }
        auto numRowsInBatch = recordBatch->num_rows();
        auto result = std::make_unique<ReadCSVMorsel>(
            currRowIdx, filePath, currRowIdxInCurrFile, std::move(recordBatch));
        currRowIdx += numRowsInBatch;
        currRowIdxInCurrFile += numRowsInBatch;
        return result;
    }
}

} // namespace processor
} // namespace kuzu
