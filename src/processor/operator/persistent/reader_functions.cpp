#include "processor/operator/persistent/reader_functions.h"

#include "common/exception/copy.h"
#include "common/system_message.h"
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

count_rows_func_t ReaderFunctions::getCountRowsFunc(
    const ReaderConfig& config, TableType tableType) {
    switch (config.fileType) {
    case FileType::CSV: {
        return tableType == TableType::REL ? countRowsNoOp : countRowsInCSVFile;
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

row_idx_t ReaderFunctions::countRowsInRelParquetFile(
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    row_idx_t numRows = 0;
    for (const auto& path : config.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(path, config);
        numRows += (row_idx_t)reader->parquet_reader()->metadata()->num_rows();
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
    assert(config.getNumFiles() != 0);
    auto reader = make_unique<NpyReader>(config.filePaths[0]);
    return reader->getNumRows();
}

row_idx_t ReaderFunctions::countRowsInRDFFile(
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    assert(config.getNumFiles() == 1);
    auto reader = make_unique<RDFReader>(config.filePaths[0], config.rdfReaderConfig->copy());
    return reader->countLine();
}

void ReaderFunctions::initRelCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RelCSVReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createRelTableCSVReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initSerialCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<SerialCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<SerialCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initParallelCSVReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<ParallelCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<ParallelCSVReader>(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initRelParquetReadData(ReaderFunctionData& funcData, vector_idx_t fileIdx,
    const common::ReaderConfig& config, MemoryManager* /*memoryManager*/) {
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

void ReaderFunctions::readRowsFromRelCSVFile(const kuzu::processor::ReaderFunctionData& funcData,
    common::block_idx_t /*blockIdx*/, common::DataChunk* dataChunkToRead) {
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
    if (blockIdx >= readerData.reader->num_row_groups()) {
        dataChunkToRead->state->selVector->selectedSize = 0;
        return;
    }
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
