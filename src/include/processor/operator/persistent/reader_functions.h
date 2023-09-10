#pragma once

#include "processor/operator/persistent/csv_reader.h"
#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace processor {

// TODO(Xiyang): Move functors to system built-in functions.
struct ReaderFunctionData {
    common::CSVReaderConfig csvReaderConfig;
    catalog::TableSchema* tableSchema;
    common::vector_idx_t fileIdx;

    virtual ~ReaderFunctionData() = default;
};

struct RelCSVReaderFunctionData : public ReaderFunctionData {
    std::shared_ptr<arrow::csv::StreamingReader> reader = nullptr;
};

struct NodeCSVReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<BufferedCSVReader> reader = nullptr;
};

struct ParquetReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<parquet::arrow::FileReader> reader = nullptr;
};

struct NPYReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<storage::NpyMultiFileReader> reader = nullptr;
};

struct FileBlocksInfo {
    common::row_idx_t numRows = 0;
    common::block_idx_t numBlocks = 0;
};

using validate_func_t =
    std::function<void(const std::vector<std::string>& paths, catalog::TableSchema* tableSchema)>;
using init_reader_data_func_t = std::function<void(ReaderFunctionData& funcData,
    const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
    common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using count_blocks_func_t = std::function<std::vector<FileBlocksInfo>(
    const std::vector<std::string>& paths, common::CSVReaderConfig csvReaderConfig,
    catalog::TableSchema* tableSchema, storage::MemoryManager* memoryManager)>;
using read_rows_func_t = std::function<void(
    const ReaderFunctionData& funcData, common::block_idx_t blockIdx, common::DataChunk*)>;

struct ReaderFunctions {
    static validate_func_t getValidateFunc(common::CopyDescription::FileType fileType);
    static count_blocks_func_t getCountBlocksFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);
    static init_reader_data_func_t getInitDataFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);
    static read_rows_func_t getReadRowsFunc(
        common::CopyDescription::FileType fileType, common::TableType tableType);
    static std::shared_ptr<ReaderFunctionData> getReadFuncData(
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

    static void initRelCSVReadData(ReaderFunctionData& funcData,
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static void initNodeCSVReadData(ReaderFunctionData& funcData,
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static void initParquetReadData(ReaderFunctionData& funcData,
        const std::vector<std::string>& paths, common::vector_idx_t fileIdx,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static void initNPYReadData(ReaderFunctionData& funcData, const std::vector<std::string>& paths,
        common::vector_idx_t fileIdx, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema);

    static void readRowsFromRelCSVFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromNodeCSVFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromParquetFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromNPYFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);

    static std::unique_ptr<common::DataChunk> getDataChunkToRead(
        catalog::TableSchema* tableSchema, storage::MemoryManager* memoryManager);
};

} // namespace processor
} // namespace kuzu
