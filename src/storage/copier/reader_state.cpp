#include "storage/copier/reader_state.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

validate_func_t ReaderFunctions::getValidateFunc(CopyDescription::FileType fileType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV:
        return validateCSVFiles;
    case CopyDescription::FileType::PARQUET:
        return validateParquetFiles;
    case CopyDescription::FileType::NPY:
        return validateNPYFiles;
    default:
        throw NotImplementedException{"ReaderFunctions::getValidateFunc"};
    }
}

count_blocks_func_t ReaderFunctions::getCountBlocksFunc(CopyDescription::FileType fileType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV:
        return countRowsInCSVFile;
    case CopyDescription::FileType::PARQUET:
        return countRowsInParquetFile;
    case CopyDescription::FileType::NPY:
        return countRowsInNPYFile;
    default:
        throw NotImplementedException{"ReaderFunctions::getRowsCounterFunc"};
    }
}

init_reader_data_func_t ReaderFunctions::getInitDataFunc(CopyDescription::FileType fileType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV:
        return initCSVReadData;
    case CopyDescription::FileType::PARQUET:
        return initParquetReadData;
    case CopyDescription::FileType::NPY:
        return initNPYReadData;
    default:
        throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
    }
}

read_rows_func_t ReaderFunctions::getReadRowsFunc(CopyDescription::FileType fileType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV:
        return readRowsFromCSVFile;
    case CopyDescription::FileType::PARQUET:
        return readRowsFromParquetFile;
    case CopyDescription::FileType::NPY:
        return readRowsFromNPYFile;
    default:
        throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
    }
}

void ReaderFunctions::validateNPYFiles(std::vector<std::string>& paths, TableSchema* tableSchema) {
    assert(!paths.empty() && paths.size() == tableSchema->getNumProperties());
    row_idx_t numRows;
    for (auto i = 0u; i < paths.size(); i++) {
        auto reader = make_unique<NpyReader>(paths[i]);
        if (i == 0) {
            numRows = reader->getNumRows();
        }
        auto tableType = tableSchema->getProperty(i)->getDataType();
        reader->validate(*tableType, numRows, tableSchema->tableName);
    }
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInCSVFile(
    std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig, TableSchema* tableSchema) {
    std::vector<FileBlocksInfo> result(paths.size());
    for (auto i = 0u; i < paths.size(); i++) {
        auto csvStreamingReader =
            TableCopyUtils::createCSVReader(paths[i], &csvReaderConfig, tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        row_idx_t numRows = 0;
        std::vector<row_idx_t> blocks;
        while (true) {
            TableCopyUtils::throwCopyExceptionIfNotOK(csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == nullptr) {
                break;
            }
            auto numRowsInBatch = currBatch->num_rows();
            numRows += numRowsInBatch;
            blocks.push_back(numRowsInBatch);
        }
        result[i] = {numRows, blocks};
    }
    return result;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInParquetFile(
    std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig, TableSchema* tableSchema) {
    std::vector<FileBlocksInfo> result(paths.size());
    for (auto i = 0u; i < paths.size(); i++) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(paths[i], tableSchema);
        auto metadata = reader->parquet_reader()->metadata();
        auto numBlocks = metadata->num_row_groups();
        std::vector<row_idx_t> blocks;
        blocks.reserve(numBlocks);
        for (auto blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
            blocks.push_back(metadata->RowGroup(blockIdx)->num_rows());
        }
        result[i] = {(row_idx_t)metadata->num_rows(), blocks};
    }
    return result;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNPYFile(
    std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig, TableSchema* tableSchema) {
    assert(!paths.empty());
    auto reader = make_unique<NpyReader>(paths[0]);
    auto numRows = reader->getNumRows();
    auto numBlocks = (block_idx_t)((numRows + CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY) /
                                   CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
    std::vector<uint64_t> blocks(numBlocks, CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
    return {FileBlocksInfo{numRows, blocks}};
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initCSVReadData(
    std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    auto reader = TableCopyUtils::createCSVReader(paths[fileIdx], &csvReaderConfig, tableSchema);
    return std::make_unique<CSVReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initParquetReadData(
    std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    auto reader = TableCopyUtils::createParquetReader(paths[fileIdx], tableSchema);
    return std::make_unique<ParquetReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initNPYReadData(
    std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    auto reader = make_unique<NpyMultiFileReader>(paths);
    return std::make_unique<NPYReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

arrow::RecordBatchVector ReaderFunctions::readRowsFromCSVFile(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx) {
    auto& readerData = (CSVReaderFunctionData&)(functionData);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(readerData.reader->ReadNext(&recordBatch));
    assert(recordBatch);
    arrow::RecordBatchVector result{recordBatch};
    return result;
}

arrow::RecordBatchVector ReaderFunctions::readRowsFromParquetFile(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx) {
    auto& readerData = (ParquetReaderFunctionData&)(functionData);
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        readerData.reader->RowGroup(static_cast<int>(blockIdx))->ReadTable(&table));
    assert(table);
    arrow::TableBatchReader batchReader(*table);
    arrow::RecordBatchVector result;
    TableCopyUtils::throwCopyExceptionIfNotOK(batchReader.ToRecordBatches().Value(&result));
    return result;
}

arrow::RecordBatchVector ReaderFunctions::readRowsFromNPYFile(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx) {
    auto& readerData = (NPYReaderFunctionData&)(functionData);
    auto recordBatch = readerData.reader->readBlock(blockIdx);
    arrow::RecordBatchVector result{recordBatch};
    return result;
}

void ReaderSharedState::validate() {
    validateFunc(filePaths, tableSchema);
}

void ReaderSharedState::countBlocks() {
    readFuncData = initFunc(filePaths, 0 /* fileIdx */, csvReaderConfig, tableSchema);
    blockInfos = countBlocksFunc(filePaths, csvReaderConfig, tableSchema);
    for (auto& blockInfo : blockInfos) {
        numRows += blockInfo.numRows;
    }
}

std::unique_ptr<ReaderMorsel> ReaderSharedState::getSerialMorsel() {
    std::unique_lock xLck{mtx};
    while (leftNumRows < StorageConstants::NODE_GROUP_SIZE) {
        auto morsel = getMorselOfNextBlock();
        if (morsel->fileIdx >= filePaths.size()) {
            // No more blocks.
            break;
        }
        if (morsel->fileIdx != readFuncData->fileIdx) {
            readFuncData = initFunc(filePaths, morsel->fileIdx, csvReaderConfig, tableSchema);
        }
        auto batchVector = readFunc(*readFuncData, morsel->blockIdx);
        for (auto& batch : batchVector) {
            leftNumRows += batch->num_rows();
            leftRecordBatches.push_back(std::move(batch));
        }
    }
    if (leftNumRows == 0) {
        return std::make_unique<ReaderMorsel>();
    }
    auto table = constructTableFromBatches(leftRecordBatches);
    leftNumRows -= table->num_rows();
    auto result =
        std::make_unique<SerialReaderMorsel>(currFileIdx, currBlockIdx, currRowIdx, table);
    currRowIdx += table->num_rows();
    return result;
}

std::unique_ptr<ReaderMorsel> ReaderSharedState::getParallelMorsel() {
    std::unique_lock xLck{mtx};
    while (true) {
        auto morsel = getMorselOfNextBlock();
        if (morsel->fileIdx >= filePaths.size()) {
            // No more blocks.
            break;
        }
        assert(morsel->fileIdx < blockInfos.size() &&
               morsel->blockIdx < blockInfos[morsel->fileIdx].numRowsPerBlock.size());
        currRowIdx += blockInfos[morsel->fileIdx].numRowsPerBlock[morsel->blockIdx];
        return morsel;
    }
    return std::make_unique<ReaderMorsel>();
}

std::unique_ptr<ReaderMorsel> ReaderSharedState::getMorselOfNextBlock() {
    if (currFileIdx >= blockInfos.size()) {
        return std::make_unique<ReaderMorsel>(currFileIdx, INVALID_BLOCK_IDX, INVALID_ROW_IDX);
    }
    auto numBlocksInFile = blockInfos[currFileIdx].numRowsPerBlock.size();
    if (currBlockIdx >= numBlocksInFile) {
        currFileIdx += fileType == CopyDescription::FileType::NPY ? filePaths.size() : 1;
        currBlockIdx = 0;
    }
    return std::make_unique<ReaderMorsel>(currFileIdx, currBlockIdx++, currRowIdx);
}

std::shared_ptr<arrow::Table> ReaderSharedState::constructTableFromBatches(
    std::vector<std::shared_ptr<arrow::RecordBatch>>& recordBatches) {
    std::shared_ptr<arrow::Table> table;
    std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatchesForTable;
    row_idx_t numRowsInTable = 0;
    while (numRowsInTable < StorageConstants::NODE_GROUP_SIZE && !recordBatches.empty()) {
        auto& currBatch = recordBatches.front();
        auto numRowsInBatch = currBatch->num_rows();
        if (numRowsInTable + numRowsInBatch > StorageConstants::NODE_GROUP_SIZE) {
            auto numRowsToAppend = StorageConstants::NODE_GROUP_SIZE - numRowsInTable;
            auto slicedBatch = currBatch->Slice(0, (int64_t)numRowsToAppend);
            recordBatchesForTable.push_back(slicedBatch);
            recordBatches.front() = currBatch->Slice((int64_t)numRowsToAppend);
            break;
        }
        recordBatchesForTable.push_back(std::move(recordBatches[0]));
        numRowsInTable += numRowsInBatch;
        recordBatches.erase(recordBatches.begin());
    }
    TableCopyUtils::throwCopyExceptionIfNotOK(
        arrow::Table::FromRecordBatches(recordBatchesForTable).Value(&table));
    return table;
}

} // namespace storage
} // namespace kuzu
