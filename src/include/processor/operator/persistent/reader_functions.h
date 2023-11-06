#pragma once

#include <functional>

#include "common/enums/table_type.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy_reader.h"
#include "processor/operator/persistent/reader/parquet/parquet_reader.h"
#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

namespace kuzu {
namespace processor {

struct ReaderFunctionData {
    common::CSVReaderConfig csvReaderConfig;
    common::vector_idx_t fileIdx;

    virtual ~ReaderFunctionData() = default;

    // Called after receiving an empty block from readFunc.
    virtual inline bool doneAfterEmptyBlock() const { return true; }

    // Called to determine if the current block has more data.
    virtual inline bool hasMoreToRead() const { return false; }
};

struct SerialCSVReaderFunctionData final : public ReaderFunctionData {
    std::unique_ptr<SerialCSVReader> reader = nullptr;
};

struct ParallelCSVReaderFunctionData final : public ReaderFunctionData {
    std::unique_ptr<ParallelCSVReader> reader = nullptr;

    // NOTE: It is *critical* that `doneAfterEmptyBlock` is false for Parallel CSV Reader!
    // Otherwise, when the parallel CSV reader gets a block that resides in the middle of a header
    // or a long line, it will return zero and cause rows to not be loaded!
    inline bool doneAfterEmptyBlock() const override { return reader->isEOF(); }
    inline bool hasMoreToRead() const override { return reader->hasMoreToRead(); }
};

struct ParquetReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<ParquetReader> reader = nullptr;
    std::unique_ptr<ParquetReaderScanState> state = nullptr;

    inline bool hasMoreToRead() const override { return !state->groupIdxList.empty(); }
};

struct NPYReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<NpyMultiFileReader> reader = nullptr;
};

struct RDFReaderFunctionData : public ReaderFunctionData {
    std::unique_ptr<RDFReader> reader = nullptr;
};

using validate_func_t = std::function<void(const common::ReaderConfig& config)>;
using init_reader_data_func_t =
    std::function<void(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager)>;
using count_rows_func_t = std::function<common::row_idx_t(
    const common::ReaderConfig& config, storage::MemoryManager* memoryManager)>;
using read_rows_func_t = std::function<void(
    ReaderFunctionData& funcData, common::block_idx_t blockIdx, common::DataChunk*)>;

struct ReaderFunctions {
    static validate_func_t getValidateFunc(common::FileType fileType);
    static count_rows_func_t getCountRowsFunc(
        const common::ReaderConfig& config, common::TableType tableType);
    static init_reader_data_func_t getInitDataFunc(const common::ReaderConfig& config);
    static read_rows_func_t getReadRowsFunc(const common::ReaderConfig& config);
    static std::shared_ptr<ReaderFunctionData> getReadFuncData(const common::ReaderConfig& config);

    static inline void validateNoOp(const common::ReaderConfig& /*config*/) {
        // DO NOTHING.
    }
    static void validateNPYFiles(const common::ReaderConfig& config);

    static common::row_idx_t countRowsNoOp(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static common::row_idx_t countRowsInCSVFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static common::row_idx_t countRowsInParquetFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static common::row_idx_t countRowsInNPYFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static common::row_idx_t countRowsInRDFFile(
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);

    static void initParallelCSVReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static void initSerialCSVReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static void initParquetReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static void initNPYReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);
    static void initRDFReadData(ReaderFunctionData& funcData, common::vector_idx_t fileIdx,
        const common::ReaderConfig& config, storage::MemoryManager* memoryManager);

    static void readRowsFromSerialCSVFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromParallelCSVFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead);
    static void readRowsFromParquetFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromNPYFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromRDFFile(const ReaderFunctionData& funcData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
};

} // namespace processor
} // namespace kuzu
