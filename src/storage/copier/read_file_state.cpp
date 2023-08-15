#include "storage/copier/read_file_state.h"

#include "storage/copier/npy_reader.h"
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void ReadCSVSharedState::countNumRows() {
    for (auto& filePath : filePaths) {
        auto csvStreamingReader =
            TableCopyUtils::createCSVReader(filePath, &csvReaderConfig, tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numRowsPerBlock;
        while (true) {
            TableCopyUtils::throwCopyExceptionIfNotOK(csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == nullptr) {
                break;
            }
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numRowsPerBlock.push_back(currNumRows);
            numRows += currNumRows;
        }
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numRowsPerBlock});
    }
}

// TODO(Guodong): Refactor duplicated between the three getMorsel() functions.
std::unique_ptr<ReadFileMorsel> ReadCSVSharedState::getMorselSerial() {
    std::unique_lock lck{mtx};
    std::shared_ptr<arrow::Table> resultTable;
    if (leftOverData) {
        resultTable = std::move(leftOverData);
        leftOverData = nullptr;
    }
    while (true) {
        if (currFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[currFileIdx];
        if (!reader) {
            reader = TableCopyUtils::createCSVReader(filePath, &csvReaderConfig, tableSchema);
        }
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        TableCopyUtils::throwCopyExceptionIfNotOK(reader->ReadNext(&recordBatch));
        if (recordBatch == nullptr) {
            // No more blocks to read in this file.
            currFileIdx++;
            currBlockIdx = 0;
            currRowIdxInCurrFile = 1;
            reader.reset();
            if (currFileIdx >= filePaths.size()) {
                break;
            }
            continue;
        }
        auto numRowsInBatch = recordBatch->num_rows();
        rowsRead += numRowsInBatch;
        currRowIdxInCurrFile += numRowsInBatch;
        currBlockIdx++;
        if (resultTable == nullptr) {
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::Table::FromRecordBatches({recordBatch}).Value(&resultTable));
        } else {
            std::shared_ptr<arrow::Table> interimTable;
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::Table::FromRecordBatches({recordBatch}).Value(&interimTable));
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::ConcatenateTables({resultTable, interimTable}).Value(&resultTable));
        }
        if (resultTable->column(0)->length() >= common::StorageConstants::NODE_GROUP_SIZE) {
            leftOverData = resultTable->Slice(common::StorageConstants::NODE_GROUP_SIZE);
            resultTable = resultTable->Slice(0, common::StorageConstants::NODE_GROUP_SIZE);
            break;
        }
        if (currFileIdx >= filePaths.size()) {
            break;
        }
    }
    auto rows = resultTable->num_rows();
    auto result = std::make_unique<ReadSerialMorsel>(
        currRowIdx, filePaths[0], currRowIdxInCurrFile, rowsRead, std::move(resultTable));
    currRowIdx += rows;
    return result;
}

std::unique_ptr<ReadFileMorsel> ReadCSVSharedState::getMorsel() {
    return std::move(getMorselSerial());
}

void ReadParquetSharedState::countNumRows() {
    for (auto& filePath : filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(filePath, tableSchema);
        auto metadata = reader->parquet_reader()->metadata();
        uint64_t numBlocks = metadata->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            numLinesPerBlock[blockIdx] = metadata->RowGroup(blockIdx)->num_rows();
        }
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numLinesPerBlock});
        numRows += metadata->num_rows();
    }
}

std::unique_ptr<ReadFileMorsel> ReadParquetSharedState::getMorselSerial() {
    std::unique_lock lck{mtx};
    std::shared_ptr<arrow::Table> resultTable;
    if (leftOverData) {
        resultTable = std::move(leftOverData);
        leftOverData = nullptr;
    }
    while (true) {
        if (currFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[currFileIdx];
        if (!reader) {
            reader = TableCopyUtils::createParquetReader(filePath, tableSchema);
        }
        std::shared_ptr<arrow::Table> table;
        TableCopyUtils::throwCopyExceptionIfNotOK(reader->ReadTable(&table));
        if (table == nullptr) {
            // No more blocks to read in this file.
            currFileIdx++;
            currBlockIdx = 0;
            reader.reset();
            if (currFileIdx >= filePaths.size()) {
                break;
            }
            continue;
        }
        auto numRowsInBatch = table->num_rows();
        rowsRead += numRowsInBatch;
        currRowIdxInCurrFile += numRowsInBatch;
        currBlockIdx++;
        if (resultTable == nullptr) {
            resultTable = table;
        } else {
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::ConcatenateTables({resultTable, table}).Value(&resultTable));
        }
        if (resultTable->column(0)->length() < common::StorageConstants::NODE_GROUP_SIZE) {
            leftOverData = resultTable->Slice(common::StorageConstants::NODE_GROUP_SIZE);
            resultTable = resultTable->Slice(0, common::StorageConstants::NODE_GROUP_SIZE);
            break;
        }
        if (currFileIdx >= filePaths.size()) {
            break;
        }
    }
    auto rows = resultTable->num_rows();
    auto result = std::make_unique<ReadSerialMorsel>(
        currRowIdx, filePaths[0], currRowIdxInCurrFile, rowsRead, std::move(resultTable));
    currRowIdx += rows;
    return result;
}

// TODO(Guodong): Refactor duplicated between the three getMorsel() functions.
std::unique_ptr<ReadFileMorsel> ReadParquetSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (currFileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[currFileIdx];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (currBlockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read in this file.
            currFileIdx++;
            currBlockIdx = 0;
            currRowIdxInCurrFile = 1;
            continue;
        }
        auto numRowsInBlock = fileBlockInfo.numRowsPerBlock[currBlockIdx];
        auto result = std::make_unique<ReadFileMorsel>(
            currRowIdx, currBlockIdx, numRowsInBlock, filePath, currRowIdxInCurrFile);
        currRowIdx += numRowsInBlock;
        currRowIdxInCurrFile += numRowsInBlock;
        currBlockIdx++;
        return result;
    }
}

void ReadNPYSharedState::countNumRows() {
    uint8_t idx = 0;
    uint64_t firstFileRows;
    for (auto& filePath : filePaths) {
        auto reader = make_unique<NpyReader>(filePath);
        numRows = reader->getNumRows();
        if (idx == 0) {
            firstFileRows = numRows;
        }
        auto tableType = tableSchema->getProperty(idx)->getDataType();
        reader->validate(*tableType, firstFileRows, tableSchema->tableName);
        auto numBlocks = (uint64_t)((numRows + CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY) /
                                    CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
        std::vector<uint64_t> numRowsPerBlock(numBlocks, CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numRowsPerBlock});
        idx++;
    }
}

std::unique_ptr<ReadFileMorsel> ReadNPYSharedState::getMorselSerial() {
    std::unique_lock lck{mtx};
    std::shared_ptr<arrow::Table> resultTable;
    if (leftOverData) {
        resultTable = std::move(leftOverData);
        leftOverData = nullptr;
    }
    while (true) {
        std::shared_ptr<arrow::RecordBatch> recordBatch = reader->readBlock(currBlockIdx);
        if (recordBatch == nullptr) {
            // No more blocks to read
            break;
        }
        auto numRowsInBatch = recordBatch->num_rows();
        rowsRead += numRowsInBatch;
        currRowIdxInCurrFile += numRowsInBatch;
        currBlockIdx++;
        if (resultTable == nullptr) {
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::Table::FromRecordBatches({recordBatch}).Value(&resultTable));
        } else {
            std::shared_ptr<arrow::Table> interimTable;
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::Table::FromRecordBatches({recordBatch}).Value(&interimTable));
            TableCopyUtils::throwCopyExceptionIfNotOK(
                arrow::ConcatenateTables({resultTable, interimTable}).Value(&resultTable));
        }
        if (resultTable->column(0)->length() >= common::StorageConstants::NODE_GROUP_SIZE) {
            leftOverData = resultTable->Slice(common::StorageConstants::NODE_GROUP_SIZE);
            resultTable = resultTable->Slice(0, common::StorageConstants::NODE_GROUP_SIZE);
        }
        break;
    }
    auto rows = resultTable->num_rows();
    auto result = std::make_unique<ReadSerialMorsel>(
        currRowIdx, "", currRowIdxInCurrFile, rowsRead, std::move(resultTable));
    currRowIdx += rows;
    return result;
}

// TODO(Guodong): Refactor duplicated between the three getMorsel() functions.
std::unique_ptr<ReadFileMorsel> ReadNPYSharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        auto filePath = filePaths[0];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (currBlockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read.
            return nullptr;
        }
        auto numRowsInBlock = fileBlockInfo.numRowsPerBlock[currBlockIdx];
        auto result = std::make_unique<ReadFileMorsel>(
            currRowIdx, currBlockIdx, numRowsInBlock, filePath, currRowIdxInCurrFile);
        currRowIdx += numRowsInBlock;
        currRowIdxInCurrFile += numRowsInBlock;
        currBlockIdx++;
        return result;
    }
}

} // namespace storage
} // namespace kuzu
