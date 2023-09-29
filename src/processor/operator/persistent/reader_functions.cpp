#include "processor/operator/persistent/reader_functions.h"

#include "common/exception/copy.h"
#include <arrow/table.h>

using namespace kuzu::common;
using namespace kuzu::catalog;
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

count_blocks_func_t ReaderFunctions::getCountBlocksFunc(
    const ReaderConfig& config, TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        if (tableType == TableType::REL) {
            return countRowsNoOp;
        } else {
            return config.csvReaderConfig->parallel ? countRowsInParallelCSVFile :
                                                      countRowsInSerialCSVFile;
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
        case TableType::UNKNOWN:
            return countRowsInParquetFile;
        case TableType::REL:
            return countRowsInRelParquetFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
        }
    }
    case FileType::NPY: {
        return countRowsInNPYFile;
    }
    case FileType::TURTLE: {
        return countRowsInRDFFile;
    }
    default: { // LCOV_EXCL_START
        throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
    } // LCOV_EXCL_END
    }
}

init_reader_data_func_t ReaderFunctions::getInitDataFunc(
    const ReaderConfig& config, TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        if (tableType == TableType::REL) {
            return initRelCSVReadData;
        } else {
            return config.csvReaderConfig->parallel ? initParallelCSVReadData :
                                                      initSerialCSVReadData;
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
        case TableType::UNKNOWN:
            return initParquetReadData;
        case TableType::REL:
            return initRelParquetReadData;
        default:
            throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
        }
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

read_rows_func_t ReaderFunctions::getReadRowsFunc(
    const ReaderConfig& config, common::TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        if (tableType == TableType::REL) {
            return readRowsFromRelCSVFile;
        } else {
            return config.csvReaderConfig->parallel ? readRowsFromParallelCSVFile :
                                                      readRowsFromSerialCSVFile;
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
        case TableType::UNKNOWN:
            return readRowsFromParquetFile;
        case TableType::REL:
            return readRowsFromRelParquetFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
        }
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

std::shared_ptr<ReaderFunctionData> ReaderFunctions::getReadFuncData(
    const ReaderConfig& config, TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        if (tableType == TableType::REL) {
            return std::make_shared<RelCSVReaderFunctionData>();
        } else if (config.csvReaderConfig->parallel) {
            return std::make_shared<ParallelCSVReaderFunctionData>();
        } else {
            return std::make_shared<SerialCSVReaderFunctionData>();
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
        case TableType::UNKNOWN:
            return std::make_shared<ParquetReaderFunctionData>();
        case TableType::REL:
            return std::make_shared<RelParquetReaderFunctionData>();
        default:
            throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
        }
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
    assert(!config.filePaths.empty() && config.getNumFiles() == config.getNumColumns());
    row_idx_t numRows;
    for (auto i = 0u; i < config.getNumFiles(); i++) {
        auto reader = make_unique<NpyReader>(config.filePaths[i]);
        if (i == 0) {
            numRows = reader->getNumRows();
        }
        reader->validate(*config.columnTypes[i], numRows);
    }
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsNoOp(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos(
        config.getNumFiles(), {INVALID_ROW_IDX, INVALID_BLOCK_IDX});
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInSerialCSVFile(
    const common::ReaderConfig& config, storage::MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.getNumFiles());
    for (const auto& path : config.filePaths) {
        auto reader = make_unique<SerialCSVReader>(path, config);
        row_idx_t numRowsInFile = reader->countRows();
        block_idx_t numBlocks =
            (numRowsInFile + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY;
        FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInParallelCSVFile(
    const common::ReaderConfig& config, storage::MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.getNumFiles());
    for (const auto& path : config.filePaths) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            throw CopyException(
                StringUtils::string_format("Failed to open file {}: {}", path, strerror(errno)));
        }
        uint64_t length = lseek(fd, 0, SEEK_END);
        close(fd);
        if (length == -1) {
            throw CopyException(StringUtils::string_format(
                "Failed to seek to end of file {}: {}", path, strerror(errno)));
        }
        auto reader = make_unique<ParallelCSVReader>(path, config);
        row_idx_t numRowsInFile = reader->countRows();
        block_idx_t numBlocks =
            (length + CopyConstants::PARALLEL_BLOCK_SIZE - 1) / CopyConstants::PARALLEL_BLOCK_SIZE;
        FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRelParquetFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.getNumFiles());
    for (const auto& path : config.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(path, config);
        auto metadata = reader->parquet_reader()->metadata();
        FileBlocksInfo fileBlocksInfo{
            (row_idx_t)metadata->num_rows(), (block_idx_t)metadata->num_row_groups()};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInParquetFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.filePaths.size());
    for (const auto& path : config.filePaths) {
        auto reader = std::make_unique<ParquetReader>(path, memoryManager);
        auto numRows = reader->getMetadata()->num_rows;
        FileBlocksInfo fileBlocksInfo{
            (row_idx_t)numRows, (block_idx_t)reader->getMetadata()->row_groups.size()};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNPYFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(config.getNumFiles() != 0);
    auto reader = make_unique<NpyReader>(config.filePaths[0]);
    auto numRows = reader->getNumRows();
    auto numBlocks =
        (block_idx_t)((numRows + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY);
    return {{numRows, numBlocks}};
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRDFFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(config.getNumFiles() == 1);
    auto reader = make_unique<RDFReader>(config.filePaths[0]);
    auto dataChunk = std::make_unique<DataChunk>(3);
    dataChunk->insert(0, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(1, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(2, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    row_idx_t numRowsInFile = 0;
    block_idx_t numBlocks = 0;
    while (true) {
        dataChunk->resetAuxiliaryBuffer();
        auto numRowsRead = reader->read(dataChunk.get());
        if (numRowsRead == 0) {
            break;
        }
        numRowsInFile += numRowsRead;
        numBlocks++;
    }
    FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
    return {fileBlocksInfo};
}

void ReaderFunctions::initRelCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RelCSVReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createRelTableCSVReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initSerialCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<SerialCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<SerialCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initParallelCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<ParallelCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<ParallelCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initRelParquetReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RelParquetReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createParquetReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initParquetReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<ParquetReaderFunctionData&>(funcData).reader =
        std::make_unique<ParquetReader>(config.filePaths[fileIdx], memoryManager);
    reinterpret_cast<ParquetReaderFunctionData&>(funcData).state =
        std::make_unique<ParquetReaderScanState>();
}

void ReaderFunctions::initNPYReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<NPYReaderFunctionData&>(funcData).reader =
        make_unique<NpyMultiFileReader>(config.filePaths);
}

void ReaderFunctions::initRDFReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RDFReaderFunctionData&>(funcData).reader =
        make_unique<RDFReader>(config.filePaths[0]);
}

void ReaderFunctions::readRowsFromRelCSVFile(const kuzu::processor::ReaderFunctionData& funcData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RelCSVReaderFunctionData&>(funcData);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(readerData.reader->ReadNext(&recordBatch));
    if (recordBatch == nullptr) {
        dataChunkToRead->state->selVector->selectedSize = 0;
        return;
    }
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
    dataChunkToRead->state->selVector->selectedSize = recordBatch->num_rows();
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

void ReaderFunctions::readRowsFromRelParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RelParquetReaderFunctionData&>(functionData);
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        readerData.reader->RowGroup(static_cast<int>(blockIdx))->ReadTable(&table));
    assert(table);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(
            dataChunkToRead->getValueVector(i).get(), table->column((int)i));
    }
    dataChunkToRead->state->selVector->selectedSize = table->num_rows();
}

void ReaderFunctions::readRowsFromParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const ParquetReaderFunctionData&>(functionData);
    if (blockIdx != UINT64_MAX &&
        (readerData.state->groupIdxList.empty() || readerData.state->groupIdxList[0] != blockIdx)) {
        readerData.reader->initializeScan(*readerData.state, {blockIdx});
    }
    readerData.reader->scan(*readerData.state, *dataChunkToRead);
}

void ReaderFunctions::readRowsFromNPYFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NPYReaderFunctionData&>(functionData);
    readerData.reader->readBlock(blockIdx, dataChunkToRead);
}

void ReaderFunctions::readRowsFromRDFFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RDFReaderFunctionData&>(functionData);
    readerData.reader->read(dataChunkToRead);
}

std::unique_ptr<common::DataChunk> ReaderFunctions::getDataChunkToRead(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<std::unique_ptr<ValueVector>> valueVectorsToRead;
    for (auto& dataType : config.columnTypes) {
        if (dataType->getLogicalTypeID() != LogicalTypeID::SERIAL) {
            valueVectorsToRead.emplace_back(
                std::make_unique<ValueVector>(*dataType, memoryManager));
        }
    }
    auto dataChunk = std::make_unique<DataChunk>(valueVectorsToRead.size());
    for (auto i = 0u; i < valueVectorsToRead.size(); i++) {
        dataChunk->insert(i, std::move(valueVectorsToRead[i]));
    }
    return dataChunk;
}

} // namespace processor
} // namespace kuzu
