#include "processor/operator/persistent/reader_state.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void LeftArrowArrays::appendFromDataChunk(common::DataChunk* dataChunk) {
    leftNumRows += ArrowColumnVector::getArrowColumn(dataChunk->getValueVector(0).get())->length();
    leftArrays.resize(dataChunk->getNumValueVectors());
    for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
        for (auto& array :
            ArrowColumnVector::getArrowColumn(dataChunk->getValueVector(i).get())->chunks()) {
            leftArrays[i].push_back(array);
        }
    }
}

void LeftArrowArrays::appendToDataChunk(common::DataChunk* dataChunk, uint64_t numRowsToAppend) {
    int64_t numRowsAppended = 0;
    auto& arrayVectorToComputeSlice = leftArrays[0];
    std::vector<arrow::ArrayVector> arrayVectors;
    arrayVectors.resize(leftArrays.size());
    uint64_t arrayVectorIdx;
    for (arrayVectorIdx = 0u; arrayVectorIdx < arrayVectorToComputeSlice.size(); arrayVectorIdx++) {
        if (numRowsAppended >= numRowsToAppend) {
            break;
        } else {
            auto arrayToComputeSlice = arrayVectorToComputeSlice[arrayVectorIdx];
            int64_t numRowsToAppendInCurArray = arrayToComputeSlice->length();
            if (numRowsToAppend - numRowsAppended < arrayToComputeSlice->length()) {
                numRowsToAppendInCurArray = (int64_t)numRowsToAppend - numRowsAppended;
                for (auto j = 0u; j < leftArrays.size(); j++) {
                    auto vectorToSlice = leftArrays[j][arrayVectorIdx];
                    leftArrays[j].push_back(vectorToSlice->Slice(numRowsToAppendInCurArray));
                    arrayVectors[j].push_back(vectorToSlice->Slice(0, numRowsToAppendInCurArray));
                }
            } else {
                for (auto j = 0u; j < leftArrays.size(); j++) {
                    arrayVectors[j].push_back(leftArrays[j][arrayVectorIdx]);
                }
            }
            numRowsAppended += numRowsToAppendInCurArray;
        }
    }
    for (auto& arrayVector : leftArrays) {
        arrayVector.erase(arrayVector.begin(), arrayVector.begin() + arrayVectorIdx);
    }
    for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
        auto chunkArray = std::make_shared<arrow::ChunkedArray>(std::move(arrayVectors[i]));
        ArrowColumnVector::setArrowColumn(
            dataChunk->getValueVector(i).get(), std::move(chunkArray));
    }
    dataChunk->state->selVector->selectedSize = numRowsToAppend;
    leftNumRows -= numRowsToAppend;
}

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

void ReaderFunctions::validateNPYFiles(
    const std::vector<std::string>& paths, TableSchema* tableSchema) {
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
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
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
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
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
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(!paths.empty());
    auto reader = make_unique<NpyReader>(paths[0]);
    auto numRows = reader->getNumRows();
    auto numBlocks = (block_idx_t)((numRows + CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY) /
                                   CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
    std::vector<uint64_t> blocks(numBlocks, CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
    return {FileBlocksInfo{numRows, blocks}};
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initCSVReadData(
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    auto reader = TableCopyUtils::createCSVReader(paths[fileIdx], &csvReaderConfig, tableSchema);
    return std::make_unique<CSVReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initParquetReadData(
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    auto reader = TableCopyUtils::createParquetReader(paths[fileIdx], tableSchema);
    return std::make_unique<ParquetReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

std::unique_ptr<ReaderFunctionData> ReaderFunctions::initNPYReadData(
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    auto reader = make_unique<NpyMultiFileReader>(paths);
    return std::make_unique<NPYReaderFunctionData>(
        csvReaderConfig, tableSchema, fileIdx, std::move(reader));
}

void ReaderFunctions::readRowsFromCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = (CSVReaderFunctionData&)(functionData);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(readerData.reader->ReadNext(&recordBatch));
    assert(recordBatch);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
}

void ReaderFunctions::readRowsFromParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = (ParquetReaderFunctionData&)(functionData);
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        readerData.reader->RowGroup(static_cast<int>(blockIdx))->ReadTable(&table));
    assert(table);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(
            dataChunkToRead->getValueVector(i).get(), table->column((int)i));
    }
}

void ReaderFunctions::readRowsFromNPYFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = (NPYReaderFunctionData&)(functionData);
    auto recordBatch = readerData.reader->readBlock(blockIdx);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
}

void ReaderSharedState::validate() {
    validateFunc(copyDescription->filePaths, tableSchema);
}

void ReaderSharedState::countBlocks() {
    readFuncData = initFunc(copyDescription->filePaths, 0 /* fileIdx */,
        *copyDescription->csvReaderConfig, tableSchema);
    blockInfos =
        countBlocksFunc(copyDescription->filePaths, *copyDescription->csvReaderConfig, tableSchema);
    for (auto& blockInfo : blockInfos) {
        numRows += blockInfo.numRows;
    }
}

std::unique_ptr<ReaderMorsel> ReaderSharedState::getSerialMorsel(DataChunk* dataChunk) {
    std::unique_lock xLck{mtx};
    readNextBlock(dataChunk);
    if (leftArrowArrays.getLeftNumRows() == 0) {
        dataChunk->state->selVector->selectedSize = 0;
        return std::make_unique<ReaderMorsel>();
    } else {
        auto numRowsToReturn = std::min(leftArrowArrays.getLeftNumRows(), DEFAULT_VECTOR_CAPACITY);
        leftArrowArrays.appendToDataChunk(dataChunk, numRowsToReturn);
        auto result = std::make_unique<ReaderMorsel>(currFileIdx, currBlockIdx, currRowIdx);
        currRowIdx += numRowsToReturn;
        return result;
    }
}

void ReaderSharedState::readNextBlock(common::DataChunk* dataChunk) {
    if (leftArrowArrays.getLeftNumRows() == 0) {
        auto morsel = getMorselOfNextBlock();
        if (morsel->fileIdx >= copyDescription->filePaths.size()) {
            // No more blocks.
            return;
        }
        if (morsel->fileIdx != readFuncData->fileIdx) {
            readFuncData = initFunc(copyDescription->filePaths, morsel->fileIdx,
                *copyDescription->csvReaderConfig, tableSchema);
        }
        readFunc(*readFuncData, morsel->blockIdx, dataChunk);
        leftArrowArrays.appendFromDataChunk(dataChunk);
    }
}

std::unique_ptr<ReaderMorsel> ReaderSharedState::getParallelMorsel() {
    std::unique_lock xLck{mtx};
    while (true) {
        auto morsel = getMorselOfNextBlock();
        if (morsel->fileIdx >= copyDescription->filePaths.size()) {
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
        currFileIdx += copyDescription->fileType == CopyDescription::FileType::NPY ?
                           copyDescription->filePaths.size() :
                           1;
        currBlockIdx = 0;
    }
    return std::make_unique<ReaderMorsel>(currFileIdx, currBlockIdx++, currRowIdx);
}

} // namespace processor
} // namespace kuzu
