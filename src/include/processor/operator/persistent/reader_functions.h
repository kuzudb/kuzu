#pragma once

#include "processor/operator/persistent/reader/csv_reader.h"
#include "processor/operator/persistent/reader/npy_reader.h"
#include "processor/operator/persistent/reader/rdf_reader.h"
#include "storage/store/table_copy_utils.h"
#include <arrow/csv/reader.h>
#include <parquet/arrow/reader.h>

namespace kuzu {
namespace processor {

struct ReaderFunctionData {
    common::CSVReaderConfig csvReaderConfig;
    common::vector_idx_t fileIdx;

    virtual ~ReaderFunctionData() = default;
};

struct RelCSVReaderFunctionData : public ReaderFunctionData {
    std::shared_ptr<arrow::csv::StreamingReader> reader = nullptr;
};

struct BufferedCSVReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<BufferedCSVReader> reader = nullptr;
};

struct ParquetReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<parquet::arrow::FileReader> reader = nullptr;
};

struct NPYReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<NpyMultiFileReader> reader = nullptr;
};

struct RDFReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<RDFReader> reader = nullptr;
};

struct FileBlocksInfo {
    common::row_idx_t numRows = 0;
    common::block_idx_t numBlocks = 0;
};

using validate_func_t = std::function<void(const common::ReaderConfig& config)>;
using init_reader_data_func_t = std::function<void(ReaderFunctionData& funcData,
    common::vector_idx_t fileIdx, const common::ReaderConfig& config)>;
using count_blocks_func_t = std::function<std::vector<FileBlocksInfo>(
    const common::ReaderConfig& config, storage::MemoryManager* memoryManager)>;
using read_rows_func_t = std::function<void(
    const ReaderFunctionData& funcData, common::block_idx_t blockIdx, common::DataChunk*)>;

struct ReaderFunctions {
    static validate_func_t getValidateFunc(common::FileType fileType);
    static count_blocks_func_t getCountBlocksFunc(
        common::FileType fileType, common::TableType tableType);
    static init_reader_data_func_t getInitDataFunc(
        common::FileType fileType, common::TableType tableType);
    static read_rows_func_t getReadRowsFunc(common::FileType fileType, common::TableType tableType);
    static std::shared_ptr<ReaderFunctionData> getReadFuncData(
        common::FileType fileType, common::TableType tableType);

    static inline void validateNoOp(const common::ReaderConfig& config) {
        // DO NOTHING.
    }
    static void validateNPYFiles(const common::ReaderConfig& config);

    static std::vector<FileBlocksInfo> countRowsNoOp(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInNodeCSVFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInParquetFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInNPYFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static std::vector<FileBlocksInfo> countRowsInRDFFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);

    static void initRelCSVReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config);
    static void initBufferedCSVReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config);
    static void initParquetReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config);
    static void initNPYReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config);
    static void initRDFReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config);

    static void readRowsFromRelCSVFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsWithBufferedCSVReader(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromParquetFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromNPYFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromRDFFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);

    static std::unique_ptr<common::DataChunk> getDataChunkToRead(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
};

} // namespace processor
} // namespace kuzu
