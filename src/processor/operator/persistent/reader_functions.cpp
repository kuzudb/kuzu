#include "processor/operator/persistent/reader_functions.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

validate_func_t ReaderFunctions::getValidateFunc(CopyDescription::FileType fileType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV:
        return validateCSVFiles;
    case CopyDescription::FileType::PARQUET:
        return validateParquetFiles;
    case CopyDescription::FileType::NPY:
        return validateNPYFiles;
    case CopyDescription::FileType::TURTLE:
        return validateRDFFiles;
    default:
        throw NotImplementedException{"ReaderFunctions::getValidateFunc"};
    }
}

count_blocks_func_t ReaderFunctions::getCountBlocksFunc(
    CopyDescription::FileType fileType, TableType tableType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return countRowsInNodeCSVFile;
        case TableType::REL:
            return countRowsInRelCSVFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
        }
    }
    case CopyDescription::FileType::PARQUET: {
        return countRowsInParquetFile;
    }
    case CopyDescription::FileType::NPY: {
        return countRowsInNPYFile;
    }
    case CopyDescription::FileType::TURTLE: {
        return countRowsInRDFFile;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getRowsCounterFunc"};
    }
    }
}

init_reader_data_func_t ReaderFunctions::getInitDataFunc(
    CopyDescription::FileType fileType, TableType tableType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return initNodeCSVReadData;
        case TableType::REL:
            return initRelCSVReadData;
        default:
            throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
        }
    }
    case CopyDescription::FileType::PARQUET: {
        return initParquetReadData;
    }
    case CopyDescription::FileType::NPY: {
        return initNPYReadData;
    }
    case CopyDescription::FileType::TURTLE: {
        return initRDFReadData;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
    }
    }
}

read_rows_func_t ReaderFunctions::getReadRowsFunc(
    CopyDescription::FileType fileType, common::TableType tableType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return readRowsFromNodeCSVFile;
        case TableType::REL:
            return readRowsFromRelCSVFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
        }
    }
    case CopyDescription::FileType::PARQUET: {
        return readRowsFromParquetFile;
    }
    case CopyDescription::FileType::NPY: {
        return readRowsFromNPYFile;
    }
    case CopyDescription::FileType::TURTLE: {
        return readRowsFromRDFFile;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
    }
    }
}

std::shared_ptr<ReaderFunctionData> ReaderFunctions::getReadFuncData(
    CopyDescription::FileType fileType, TableType tableType) {
    switch (fileType) {
    case CopyDescription::FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return std::make_shared<NodeCSVReaderFunctionData>();
        case TableType::REL:
            return std::make_shared<RelCSVReaderFunctionData>();
        default:
            throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
        }
    }
    case CopyDescription::FileType::PARQUET: {
        return std::make_shared<ParquetReaderFunctionData>();
    }
    case CopyDescription::FileType::NPY: {
        return std::make_shared<NPYReaderFunctionData>();
    }
    case CopyDescription::FileType::TURTLE: {
        return std::make_shared<RDFReaderFunctionData>();
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
    }
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

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRelCSVFile(
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos(paths.size(), {INVALID_ROW_IDX, INVALID_BLOCK_IDX});
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNodeCSVFile(
    const std::vector<std::string>& paths, common::CSVReaderConfig csvReaderConfig,
    catalog::TableSchema* tableSchema, storage::MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(paths.size());
    auto dataChunk = getDataChunkToRead(tableSchema, memoryManager);
    // We should add a countNumRows() API to csvReader, so that it doesn't need to read data to
    // valueVector when counting the csv file.
    for (const auto& path : paths) {
        auto reader = std::make_unique<BufferedCSVReader>(path, csvReaderConfig, tableSchema);
        row_idx_t numRowsInFile = 0;
        block_idx_t numBlocks = 0;
        while (true) {
            dataChunk->state->selVector->selectedSize = 0;
            auto numRowsRead = reader->ParseCSV(*dataChunk);
            if (numRowsRead == 0) {
                break;
            }
            numRowsInFile += numRowsRead;
            numBlocks++;
        }
        FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInParquetFile(
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(paths.size());
    for (const auto& path : paths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(path, tableSchema);
        auto metadata = reader->parquet_reader()->metadata();
        FileBlocksInfo fileBlocksInfo{
            (row_idx_t)metadata->num_rows(), (block_idx_t)metadata->num_row_groups()};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNPYFile(
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema, MemoryManager* memoryManager) {
    assert(!paths.empty());
    auto reader = make_unique<NpyReader>(paths[0]);
    auto numRows = reader->getNumRows();
    auto numBlocks =
        (block_idx_t)((numRows + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY);
    return {{numRows, numBlocks}};
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRDFFile(
    const std::vector<std::string>& paths, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema, MemoryManager* memoryManager) {
    assert(paths.size() == 1);
    auto reader = make_unique<RDFReader>(paths[0]);
    auto dataChunk = std::make_unique<DataChunk>(3);
    dataChunk->insert(0, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(1, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(2, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    row_idx_t numRowsInFile = 0;
    block_idx_t numBlocks = 0;
    while (true) {
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

void ReaderFunctions::initRelCSVReadData(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    funcData.fileIdx = fileIdx;
    funcData.tableSchema = tableSchema;
    reinterpret_cast<RelCSVReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createCSVReader(paths[fileIdx], &csvReaderConfig, tableSchema);
}

void ReaderFunctions::initNodeCSVReadData(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    funcData.fileIdx = fileIdx;
    funcData.tableSchema = tableSchema;
    reinterpret_cast<NodeCSVReaderFunctionData&>(funcData).reader =
        std::make_unique<BufferedCSVReader>(paths[fileIdx], csvReaderConfig, tableSchema);
}

void ReaderFunctions::initParquetReadData(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    assert(fileIdx < paths.size());
    funcData.fileIdx = fileIdx;
    funcData.tableSchema = tableSchema;
    reinterpret_cast<ParquetReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createParquetReader(paths[fileIdx], tableSchema);
}

void ReaderFunctions::initNPYReadData(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    funcData.fileIdx = fileIdx;
    funcData.tableSchema = tableSchema;
    reinterpret_cast<NPYReaderFunctionData&>(funcData).reader =
        make_unique<NpyMultiFileReader>(paths);
}

void ReaderFunctions::initRDFReadData(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, vector_idx_t fileIdx, CSVReaderConfig csvReaderConfig,
    TableSchema* tableSchema) {
    funcData.fileIdx = fileIdx;
    funcData.tableSchema = tableSchema;
    reinterpret_cast<RDFReaderFunctionData&>(funcData).reader = make_unique<RDFReader>(paths[0]);
}

void ReaderFunctions::readRowsFromRelCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RelCSVReaderFunctionData&>(functionData);
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

void ReaderFunctions::readRowsFromNodeCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NodeCSVReaderFunctionData&>(functionData);
    readerData.reader->ParseCSV(*dataChunkToRead);
}

void ReaderFunctions::readRowsFromParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const ParquetReaderFunctionData&>(functionData);
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

void ReaderFunctions::readRowsFromNPYFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NPYReaderFunctionData&>(functionData);
    auto recordBatch = readerData.reader->readBlock(blockIdx);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
    dataChunkToRead->state->selVector->selectedSize = recordBatch->num_rows();
}

void ReaderFunctions::readRowsFromRDFFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RDFReaderFunctionData&>(functionData);
    readerData.reader->read(dataChunkToRead);
}

std::unique_ptr<common::DataChunk> ReaderFunctions::getDataChunkToRead(
    catalog::TableSchema* tableSchema, MemoryManager* memoryManager) {
    std::vector<std::unique_ptr<ValueVector>> valueVectorsToRead;
    for (auto i = 0u; i < tableSchema->getNumProperties(); i++) {
        auto property = tableSchema->getProperty(i);
        if (property->getDataType()->getLogicalTypeID() != LogicalTypeID::SERIAL) {
            valueVectorsToRead.emplace_back(std::make_unique<ValueVector>(
                *tableSchema->getProperty(i)->getDataType(), memoryManager));
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
