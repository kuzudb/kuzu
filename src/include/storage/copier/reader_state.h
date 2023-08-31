#pragma once

#include "common/data_chunk/data_chunk.h"
#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace processor {
class Reader;
}
namespace storage {

struct ReaderFunctionData {
    ReaderFunctionData(common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema,
        common::vector_idx_t fileIdx)
        : csvReaderConfig{csvReaderConfig}, tableSchema{tableSchema}, fileIdx{fileIdx} {}
    virtual ~ReaderFunctionData() = default;

    common::CSVReaderConfig csvReaderConfig;
    catalog::TableSchema* tableSchema;
    common::vector_idx_t fileIdx;
};

struct CSVReaderFunctionData : public ReaderFunctionData {
    CSVReaderFunctionData(common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema, common::vector_idx_t fileIdx,
        std::shared_ptr<arrow::csv::StreamingReader> reader)
        : ReaderFunctionData{csvReaderConfig, tableSchema, fileIdx}, reader{std::move(reader)} {}

    std::shared_ptr<arrow::csv::StreamingReader> reader;
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
    std::function<void(std::vector<std::string>& paths, catalog::TableSchema* tableSchema)>;
using init_reader_data_func_t = std::function<std::unique_ptr<ReaderFunctionData>(
    std::vector<std::string>& paths, common::vector_idx_t fileIdx,
    common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using count_blocks_func_t =
    std::function<std::vector<FileBlocksInfo>(std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using read_rows_func_t = std::function<void(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx, common::DataChunk*)>;

struct ReaderFunctions {
    static validate_func_t getValidateFunc(common::CopyDescription::FileType fileType);
    static count_blocks_func_t getCountBlocksFunc(common::CopyDescription::FileType fileType);
    static init_reader_data_func_t getInitDataFunc(common::CopyDescription::FileType fileType);
    static read_rows_func_t getReadRowsFunc(common::CopyDescription::FileType fileType);

    static inline void validateCSVFiles(
        std::vector<std::string>& paths, catalog::TableSchema* tableSchema) {
        // DO NOTHING.
    }
    static inline void validateParquetFiles(
        std::vector<std::string>& paths, catalog::TableSchema* tableSchema) {
        // DO NOTHING.
    }
    static void validateNPYFiles(
        std::vector<std::string>& paths, catalog::TableSchema* tableSchema);

    static std::vector<FileBlocksInfo> countRowsInCSVFile(std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::vector<FileBlocksInfo> countRowsInParquetFile(std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::vector<FileBlocksInfo> countRowsInNPYFile(std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema);

    static std::unique_ptr<ReaderFunctionData> initCSVReadData(std::vector<std::string>& paths,
        common::vector_idx_t fileIdx, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema);
    static std::unique_ptr<ReaderFunctionData> initParquetReadData(std::vector<std::string>& paths,
        common::vector_idx_t fileIdx, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema);
    static std::unique_ptr<ReaderFunctionData> initNPYReadData(std::vector<std::string>& paths,
        common::vector_idx_t fileIdx, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema);

    static void readRowsFromCSVFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromParquetFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
    static void readRowsFromNPYFile(const ReaderFunctionData& functionData,
        common::block_idx_t blockIdx, common::DataChunk* vectorsToRead);
};

class ReaderSharedState {
    friend class processor::Reader;

public:
    ReaderSharedState(common::CopyDescription::FileType fileType,
        std::vector<std::string> filePaths, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema)
        : fileType{fileType}, filePaths{std::move(filePaths)}, csvReaderConfig{csvReaderConfig},
          tableSchema{tableSchema}, numRows{0}, currFileIdx{0}, currBlockIdx{0}, currRowIdx{0} {
        validateFunc = ReaderFunctions::getValidateFunc(fileType);
        initFunc = ReaderFunctions::getInitDataFunc(fileType);
        countBlocksFunc = ReaderFunctions::getCountBlocksFunc(fileType);
        readFunc = ReaderFunctions::getReadRowsFunc(fileType);
    }

    void validate();
    void countBlocks();

    std::unique_ptr<ReaderMorsel> getSerialMorsel(common::DataChunk* vectorsToRead);
    std::unique_ptr<ReaderMorsel> getParallelMorsel();

    inline void lock() { mtx.lock(); }
    inline void unlock() { mtx.unlock(); }
    inline common::row_idx_t& getNumRowsRef() { return std::ref(numRows); }

private:
    std::unique_ptr<ReaderMorsel> getMorselOfNextBlock();

public:
    std::mutex mtx;

    common::CopyDescription::FileType fileType;
    std::vector<std::string> filePaths;
    common::CSVReaderConfig csvReaderConfig;
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

} // namespace storage
} // namespace kuzu
