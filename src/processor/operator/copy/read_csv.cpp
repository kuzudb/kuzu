#include "processor/operator/copy/read_csv.h"

namespace kuzu {
namespace processor {

void ReadCSVSharedState::countNumLines() {
    for (auto& filePath : filePaths) {
        auto csvStreamingReader =
            storage::TableCopyUtils::createCSVReader(filePath, &csvReaderConfig, tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numLinesPerBlock;
        auto startNodeOffset = numRows;
        while (true) {
            storage::TableCopyUtils::throwCopyExceptionIfNotOK(
                csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == nullptr) {
                break;
            }
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numRows += currNumRows;
        }
        fileBlockInfos.emplace(
            filePath, storage::FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
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
            reader.reset();
            continue;
        }
        auto morselNodeOffset = nodeOffset;
        nodeOffset += recordBatch->num_rows();
        return std::make_unique<ReadCSVMorsel>(morselNodeOffset, filePath, std::move(recordBatch));
    }
}

} // namespace processor
} // namespace kuzu
