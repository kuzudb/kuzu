#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace processor {

void ReadNPYSharedState::countNumLines() {
    uint8_t idx = 0;
    uint64_t firstFileRows;
    for (auto& filePath : filePaths) {
        std::unique_ptr<storage::NpyReader> reader = make_unique<storage::NpyReader>(filePath);
        numRows = reader->getNumRows();
        if (idx == 0) {
            firstFileRows = numRows;
        }
        auto tableType = tableSchema->getProperty(idx).dataType;
        reader->validate(tableType, firstFileRows, tableSchema->tableName);
        auto numBlocks = (uint64_t)((numRows + common::CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY) /
                                    common::CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
        std::vector<uint64_t> numRowsPerBlock(
            numBlocks, common::CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
        fileBlockInfos.emplace(filePath, storage::FileBlockInfo{0, numBlocks, numRowsPerBlock});
        idx++;
    }
}

std::unique_ptr<ReadFileMorsel> ReadNPYSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        auto filePath = filePaths[0];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (curBlockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read
            return nullptr;
        }
        auto result = std::make_unique<ReadFileMorsel>(
            curBlockIdx, fileBlockInfo.numLinesPerBlock[curBlockIdx], filePath);
        curBlockIdx++;
        return result;
    }
}

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    if (!reader) {
        reader = std::make_unique<storage::NpyMultiFileReader>(sharedState->getFilePaths());
    }
    auto batch = reader->readBlock(morsel->blockIdx);
    return batch;
}

} // namespace processor
} // namespace kuzu
