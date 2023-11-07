#include "processor/operator/persistent/reader_functions.h"

#include "common/enums/table_type.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

validate_func_t ReaderFunctions::getValidateFunc(FileType fileType) {
    switch (fileType) {
    case FileType::NPY:
        return validateNPYFiles;
    case FileType::CSV:
    case FileType::PARQUET:
    case FileType::TURTLE:
        return validateNoOp;
    default:
        throw NotImplementedException{"ReaderFunctions::getValidateFunc"};
    }
}

count_rows_func_t ReaderFunctions::getCountRowsFunc(
    const ReaderConfig& config, TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        return tableType == TableType::REL ? countRowsNoOp : countRowsInCSVFile;
    }
    case FileType::PARQUET: {
        return countRowsInParquetFile;
    }
    case FileType::NPY: {
        return countRowsInNPYFile;
    }
    case FileType::TURTLE: {
        return countRowsInRDFFile;
    }
    default: { // LCOV_EXCL_START
        throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
    } // LCOV_EXCL_STOP
    }
}

init_reader_data_func_t ReaderFunctions::getInitDataFunc(const ReaderConfig& config) {
    switch (config.fileType) {
    case FileType::CSV: {
        return config.csvReaderConfig->parallel ? initParallelCSVReadData : initSerialCSVReadData;
    }
    case FileType::PARQUET: {
        return initParquetReadData;
    }
    case FileType::NPY: {
        return initNPYReadData;
    }
    case FileType::TURTLE: {
        return initRDFReadData;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
    }
    }
}

read_rows_func_t ReaderFunctions::getReadRowsFunc(const ReaderConfig& config) {
    switch (config.fileType) {
    case FileType::CSV: {
        return config.csvReaderConfig->parallel ? readRowsFromParallelCSVFile :
                                                  readRowsFromSerialCSVFile;
    }
    case FileType::PARQUET: {
        return readRowsFromParquetFile;
    }
    case FileType::NPY: {
        return readRowsFromNPYFile;
    }
    case FileType::TURTLE: {
        return readRowsFromRDFFile;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
    }
    }
}

std::shared_ptr<ReaderFunctionData> ReaderFunctions::getReadFuncData(const ReaderConfig& config) {
    switch (config.fileType) {
    case FileType::CSV: {
        if (config.csvReaderConfig->parallel) {
            return std::make_shared<ParallelCSVReaderFunctionData>();
        } else {
            return std::make_shared<SerialCSVReaderFunctionData>();
        }
    }
    case FileType::PARQUET: {
        return std::make_shared<ParquetReaderFunctionData>();
    }
    case FileType::NPY: {
        return std::make_shared<NPYReaderFunctionData>();
    }
    case FileType::TURTLE: {
        return std::make_shared<RDFReaderFunctionData>();
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
    }
    }
}

void ReaderFunctions::validateNPYFiles(const common::ReaderConfig& config) {
    // Validate one file for one column.
    KU_ASSERT(!config.filePaths.empty() && config.getNumFiles() == config.getNumColumns());
    row_idx_t numRows;
    for (auto i = 0u; i < config.getNumFiles(); i++) {
        auto reader = make_unique<NpyReader>(config.filePaths[i]);
        if (i == 0) {
            numRows = reader->getNumRows();
        }
        reader->validate(*config.columnTypes[i], numRows);
    }
}

row_idx_t ReaderFunctions::countRowsNoOp(
    const common::ReaderConfig& /*config*/, MemoryManager* /*memoryManager*/) {
    return INVALID_ROW_IDX;
}

row_idx_t ReaderFunctions::countRowsInCSVFile(
    const common::ReaderConfig& config, storage::MemoryManager* /*memoryManager*/) {
    row_idx_t numRows = 0;
    for (const auto& path : config.filePaths) {
        auto reader = make_unique<SerialCSVReader>(path, config);
        numRows += reader->countRows();
    }
    return numRows;
}

row_idx_t ReaderFunctions::countRowsInParquetFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    row_idx_t numRows = 0;
    for (const auto& path : config.filePaths) {
        auto reader = std::make_unique<ParquetReader>(path, memoryManager);
        numRows += reader->getMetadata()->num_rows;
    }
    return numRows;
}

row_idx_t ReaderFunctions::countRowsInNPYFile(
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    KU_ASSERT(config.getNumFiles() != 0);
    auto reader = make_unique<NpyReader>(config.filePaths[0]);
    return reader->getNumRows();
}

row_idx_t ReaderFunctions::countRowsInRDFFile(
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    KU_ASSERT(config.getNumFiles() == 1);
    auto reader = make_unique<RDFReader>(config.filePaths[0], config.rdfReaderConfig->copy());
    return reader->countLine();
}

void ReaderFunctions::initSerialCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    KU_ASSERT(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<SerialCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<SerialCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initParallelCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    KU_ASSERT(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<ParallelCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<ParallelCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initParquetReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    KU_ASSERT(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<ParquetReaderFunctionData&>(funcData).reader =
        std::make_unique<ParquetReader>(config.filePaths[fileIdx], memoryManager);
    reinterpret_cast<ParquetReaderFunctionData&>(funcData).state =
        std::make_unique<ParquetReaderScanState>();
}

void ReaderFunctions::initNPYReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<NPYReaderFunctionData&>(funcData).reader =
        make_unique<NpyMultiFileReader>(config.filePaths);
}

void ReaderFunctions::initRDFReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RDFReaderFunctionData&>(funcData).reader =
        make_unique<RDFReader>(config.filePaths[0], config.rdfReaderConfig->copy());
}

void ReaderFunctions::readRowsFromSerialCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const SerialCSVReaderFunctionData&>(functionData);
    uint64_t numRows = readerData.reader->parseBlock(blockIdx, *dataChunkToRead);
    dataChunkToRead->state->selVector->selectedSize = numRows;
}

void ReaderFunctions::readRowsFromParallelCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const ParallelCSVReaderFunctionData&>(functionData);
    uint64_t numRows;
    if (blockIdx == UINT64_MAX) {
        numRows = readerData.reader->continueBlock(*dataChunkToRead);
    } else {
        numRows = readerData.reader->parseBlock(blockIdx, *dataChunkToRead);
    }
    dataChunkToRead->state->selVector->selectedSize = numRows;
}

void ReaderFunctions::readRowsFromParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const ParquetReaderFunctionData&>(functionData);
    if (blockIdx != UINT64_MAX) {
        if (blockIdx >= readerData.reader->getMetadata()->row_groups.size()) {
            dataChunkToRead->state->selVector->selectedSize = 0;
            return;
        }
        if (readerData.state->groupIdxList.empty() ||
            readerData.state->groupIdxList[0] != blockIdx) {
            readerData.reader->initializeScan(*readerData.state, {blockIdx});
        }
    }
    readerData.reader->scan(*readerData.state, *dataChunkToRead);
}

void ReaderFunctions::readRowsFromNPYFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NPYReaderFunctionData&>(functionData);
    readerData.reader->readBlock(blockIdx, dataChunkToRead);
}

void ReaderFunctions::readRowsFromRDFFile(const ReaderFunctionData& functionData,
    common::block_idx_t /*blockIdx*/, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RDFReaderFunctionData&>(functionData);
    readerData.reader->read(dataChunkToRead);
}

} // namespace processor
} // namespace kuzu
