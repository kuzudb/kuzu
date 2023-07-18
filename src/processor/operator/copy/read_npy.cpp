#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace processor {

void ReadNPYSharedState::countNumRows() {
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
        if (curFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[curFileIdx];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (curBlockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read in this file.
            curFileIdx++;
            currRowIdxInCurrFile = 1;
            curBlockIdx = 0;
            currRowIdx = 0;
            continue;
        }
        auto numRowsInBlock = fileBlockInfo.numRowsPerBlock[curBlockIdx];
        auto result = std::make_unique<ReadNPYMorsel>(
            currRowIdx, curBlockIdx, numRowsInBlock, filePath, curFileIdx, currRowIdxInCurrFile);
        currRowIdx += numRowsInBlock;
        currRowIdxInCurrFile += numRowsInBlock;
        curBlockIdx++;
        return result;
    }
}

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<storage::NpyReader>(morsel->filePath);
    }
    auto batch = reader->readBlock(morsel->blockIdx);
    return batch;
}

bool ReadNPY::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto sharedStateNPY = reinterpret_cast<ReadNPYSharedState*>(sharedState.get());
    auto morsel = sharedStateNPY->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    auto npyMorsel = reinterpret_cast<ReadNPYMorsel*>(morsel.get());
    auto startRowIdx = npyMorsel->rowIdx;
    auto columnIdx = npyMorsel->getColumnIdx();
    rowIdxVector->setValue(rowIdxVector->state->selVector->selectedPositions[0], startRowIdx);
    columnIdxVector->setValue(columnIdxVector->state->selVector->selectedPositions[0], columnIdx);
    auto recordBatch = readTuples(std::move(morsel));
    common::ArrowColumnVector::setArrowColumn(
        arrowColumnVectors[columnIdx], recordBatch->column((int)0));
    return true;
}

void ReadNPY::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ReadFile::initLocalStateInternal(resultSet, context);
    columnIdxVector = resultSet->getValueVector(columnIdxPos).get();
}

} // namespace processor
} // namespace kuzu
