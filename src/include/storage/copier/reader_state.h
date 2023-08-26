#pragma once

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

struct SerialReaderMorsel : public ReaderMorsel {
    SerialReaderMorsel(common::vector_idx_t fileIdx, common::block_idx_t blockIdx,
        common::row_idx_t rowIdx, std::shared_ptr<arrow::Table> table)
        : ReaderMorsel{fileIdx, blockIdx, rowIdx}, table{std::move(table)} {}

    std::shared_ptr<arrow::Table> table;
};

using validate_func_t =
    std::function<void(std::vector<std::string>& paths, catalog::TableSchema* tableSchema)>;
using init_reader_data_func_t = std::function<std::unique_ptr<ReaderFunctionData>(
    std::vector<std::string>& paths, common::vector_idx_t fileIdx,
    common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using count_blocks_func_t =
    std::function<std::vector<FileBlocksInfo>(std::vector<std::string>& paths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)>;
using read_rows_func_t = std::function<arrow::RecordBatchVector(
    const ReaderFunctionData& functionData, common::block_idx_t blockIdx)>;

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

    static arrow::RecordBatchVector readRowsFromCSVFile(
        const ReaderFunctionData& functionData, common::block_idx_t blockIdx);
    static arrow::RecordBatchVector readRowsFromParquetFile(
        const ReaderFunctionData& functionData, common::block_idx_t blockIdx);
    static arrow::RecordBatchVector readRowsFromNPYFile(
        const ReaderFunctionData& functionData, common::block_idx_t blockIdx);
};

class ReaderSharedState {
    friend class processor::Reader;

public:
    ReaderSharedState(common::CopyDescription::FileType fileType,
        std::vector<std::string> filePaths, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema)
        : fileType{fileType}, filePaths{std::move(filePaths)}, csvReaderConfig{csvReaderConfig},
          tableSchema{tableSchema}, numRows{0}, currFileIdx{0}, currBlockIdx{0}, currRowIdx{0},
          leftRecordBatches{}, leftNumRows{0} {
        validateFunc = ReaderFunctions::getValidateFunc(fileType);
        initFunc = ReaderFunctions::getInitDataFunc(fileType);
        countBlocksFunc = ReaderFunctions::getCountBlocksFunc(fileType);
        readFunc = ReaderFunctions::getReadRowsFunc(fileType);
    }

    void validate();
    void countBlocks();

    std::unique_ptr<ReaderMorsel> getSerialMorsel();
    std::unique_ptr<ReaderMorsel> getParallelMorsel();

    inline void lock() { mtx.lock(); }
    inline void unlock() { mtx.unlock(); }
    inline common::row_idx_t& getNumRowsRef() { return std::ref(numRows); }

private:
    std::unique_ptr<ReaderMorsel> getMorselOfNextBlock();

    static std::shared_ptr<arrow::Table> constructTableFromBatches(
        std::vector<std::shared_ptr<arrow::RecordBatch>>& recordBatches);

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

    std::vector<std::shared_ptr<arrow::RecordBatch>> leftRecordBatches;
    common::row_idx_t leftNumRows;
};

} // namespace storage
} // namespace kuzu
