#pragma once

#include "common/data_chunk/data_chunk.h"
#include "csv_reader.h"
#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace processor {

struct ReaderFunctionData {
    ReaderFunctionData(common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        common::vector_idx_t fileIdx)
        : csvReaderConfig{csvReaderConfig}, tableSchema{tableSchema}, fileIdx{fileIdx} {}
    virtual ~ReaderFunctionData() = default;

    common::CSVReaderConfig csvReaderConfig;
    catalog::TableSchema* tableSchema;
    common::vector_idx_t fileIdx;
};

struct RelCSVReaderFunctionData : public ReaderFunctionData {
    RelCSVReaderFunctionData(common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema, common::vector_idx_t fileIdx,
        std::shared_ptr<arrow::csv::StreamingReader> reader)
        : ReaderFunctionData{csvReaderConfig, tableSchema, fileIdx}, reader{std::move(reader)} {}

    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

struct NodeCSVReaderFunctionData : public ReaderFunctionData {
    NodeCSVReaderFunctionData(common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema, common::vector_idx_t fileIdx,
        std::unique_ptr<BufferedCSVReader> csvReader)
        : ReaderFunctionData{csvReaderConfig, tableSchema, fileIdx}, csvReader{
                                                                         std::move(csvReader)} {}

    std::unique_ptr<BufferedCSVReader> csvReader;
};

struct ParquetReaderFunctionData : public ReaderFunctionData {
    ParquetReaderFunctionData(common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema, common::vector_idx_t fileIdx,
        std::unique_ptr<parquet::arrow::FileReader> reader)
        : ReaderFunctionData{csvReaderConfig, tableSchema, fileIdx}, reader{std::move(reader)} {}

    std::unique_ptr<parquet::arrow::FileReader> reader;
};

struct NPYReaderFunctionData : public ReaderFunctionData {
    NPYReaderFunctionData(common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema, common::vector_idx_t fileIdx,
        std::unique_ptr<storage::NpyMultiFileReader> reader)
        : ReaderFunctionData{csvReaderConfig, tableSchema, fileIdx}, reader{std::move(reader)} {}

    std::unique_ptr<storage::NpyMultiFileReader> reader;
};

struct FileBlocksInfo {
    common::row_idx_t numRows = 0;
    std::vector<common::row_idx_t> numRowsPerBlock;
};

struct ReaderMorsel {
    ReaderMorsel()
        : fileIdx{common::INVALID_VECTOR_IDX}, blockIdx{common::INVALID_BLOCK_IDX},
          rowIdx{common::INVALID_ROW_IDX} {}

    ReaderMorsel(
        common::vector_idx_t fileIdx, common::block_idx_t blockIdx, common::row_idx_t rowIdx)
        : fileIdx{fileIdx}, blockIdx{blockIdx}, rowIdx{rowIdx} {}

    virtual ~ReaderMorsel() = default;

    common::vector_idx_t fileIdx;
    common::block_idx_t blockIdx;
    common::row_idx_t rowIdx;
};

class LeftArrowArrays {
public:
    explicit LeftArrowArrays() : leftNumRows{0} {}

    inline uint64_t getLeftNumRows() const { return leftNumRows; }

    void appendFromDataChunk(common::DataChunk* dataChunk);

    void appendToDataChunk(common::DataChunk* dataChunk, uint64_t numRowsToAppend);

private:
    common::row_idx_t leftNumRows;
    std::vector<arrow::ArrayVector> leftArrays;
};

using validate_func_t =
    std::function<void(const std::vector<std::string>& paths, catalog::TableSchema* tableSchema)>;
using init_reader_data_func_t = std::function<std::unique_ptr<ReaderFunctionData>(
    const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
    common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using count_blocks_func_t = std::function<std::vector<FileBlocksInfo>(
    const std::vector<std::string>& paths, common::CSVReaderConfig csvReaderConfig,
    catalog::TableSchema* tableSchema, storage::MemoryManager*)>;
using read_rows_func_t = std::function<void(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx, common::DataChunk*)>;

struct ReaderFunctions {
    static validate_func_t getValidateFunc(common::CopyDescription::FileType fileType);
    static count_blocks_func_t getCountBlocksFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);
    static init_reader_data_func_t getInitDataFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);
    static read_rows_func_t getReadRowsFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);

    static inline void validateCSVFiles(
        const std::vector<std::string>& paths, catalog::TableSchema* tableSchema) {
        // DO NOTHING.
    }
    static inline void validateParquetFiles(
        const std::vector<std::string>& paths, catalog::TableSchema* tableSchema) {
        // DO NOTHING.
    }
    static void validateNPYFiles(
        const std::vector<std::string>& paths, catalog::TableSchema* tableSchema);

    static std::vector<FileBlocksInfo> countRowsInRelCSVFile(const std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInNodeCSVFile(const std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInParquetFile(const std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInNPYFile(const std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        storage::MemoryManager* memoryManager);

    static std::unique_ptr<ReaderFunctionData> initRelCSVReadData(
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<ReaderFunctionData> initNodeCSVReadData(
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<ReaderFunctionData> initParquetReadData(
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<ReaderFunctionData> initNPYReadData(
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);

    static void readRowsFromRelCSVFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromNodeCSVFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromParquetFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromNPYFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);

    static std::unique_ptr<common::DataChunk> getDataChunkToRead(
        catalog::TableSchema* tableSchema, storage::MemoryManager* memoryManager);
};

class ReaderSharedState {
    friend class Reader;

public:
    ReaderSharedState(
        std::unique_ptr<common::CopyDescription> copyDescription, catalog::TableSchema* tableSchema)
        : copyDescription{std::move(copyDescription)}, tableSchema{tableSchema}, numRows{0},
          currFileIdx{0}, currBlockIdx{0}, currRowIdx{0} {
        validateFunc = ReaderFunctions::getValidateFunc(this->copyDescription->fileType);
        initFunc = ReaderFunctions::getInitDataFunc(
            this->copyDescription->fileType, tableSchema->tableType);
        countBlocksFunc = ReaderFunctions::getCountBlocksFunc(
            this->copyDescription->fileType, tableSchema->tableType);
        readFunc = ReaderFunctions::getReadRowsFunc(
            this->copyDescription->fileType, tableSchema->tableType);
    }

    void validate();
    void countBlocks(storage::MemoryManager* memoryManager);

    std::unique_ptr<ReaderMorsel> getSerialMorsel(common::DataChunk* vectorsToRead);
    std::unique_ptr<ReaderMorsel> getParallelMorsel();

    inline void lock() { mtx.lock(); }
    inline void unlock() { mtx.unlock(); }
    inline common::row_idx_t& getNumRowsRef() { return std::ref(numRows); }

private:
    std::unique_ptr<ReaderMorsel> getMorselOfNextBlock();
    void readNextBlock(common::DataChunk* dataChunk);

public:
    std::mutex mtx;

    std::unique_ptr<common::CopyDescription> copyDescription;
    catalog::TableSchema* tableSchema;

    validate_func_t validateFunc;
    init_reader_data_func_t initFunc;
    count_blocks_func_t countBlocksFunc;
    read_rows_func_t readFunc;
    std::unique_ptr<ReaderFunctionData> readFuncData;

    common::row_idx_t numRows;
    std::vector<FileBlocksInfo> blockInfos;

    common::vector_idx_t currFileIdx;
    common::block_idx_t currBlockIdx;
    common::row_idx_t currRowIdx;

private:
    LeftArrowArrays leftArrowArrays;
};

} // namespace processor
} // namespace kuzu
