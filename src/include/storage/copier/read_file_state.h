#pragma once

#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace storage {

class ReadFileMorsel {
public:
    ReadFileMorsel(common::row_idx_t rowIdx, common::block_idx_t blockIdx,
        common::row_idx_t rowsRead, std::string filePath, common::row_idx_t rowIdxInFile)
        : rowIdx{rowIdx}, blockIdx{blockIdx}, rowsRead{rowsRead}, filePath{std::move(filePath)},
          rowIdxInFile{rowIdxInFile} {};
    virtual ~ReadFileMorsel() = default;

public:
    // When reading from multiple files, rowIdx is accumulated.
    common::row_idx_t rowIdx;
    common::block_idx_t blockIdx;
    common::row_idx_t rowsRead;
    std::string filePath;
    // Row idx in the current file. Equal to `rowIdx` when reading from only a single file.
    common::row_idx_t rowIdxInFile;
};

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class ReadSerialMorsel : public ReadFileMorsel {
public:
    ReadSerialMorsel(common::offset_t startRowIdx, std::string filePath,
        common::row_idx_t rowIdxInFile, common::row_idx_t rowsRead,
        std::shared_ptr<arrow::Table> recordTable)
        : ReadFileMorsel{startRowIdx, common::INVALID_BLOCK_IDX, rowsRead, std::move(filePath),
              rowIdxInFile},
          recordTable{std::move(recordTable)} {}

    std::shared_ptr<arrow::Table> recordTable;
};

class ReadFileSharedState {
public:
    ReadFileSharedState(std::vector<std::string> filePaths, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema)
        : numRows{0}, rowsRead{0}, tableSchema{tableSchema}, filePaths{std::move(filePaths)},
          csvReaderConfig{csvReaderConfig}, currRowIdx{0}, currBlockIdx{0}, currFileIdx{0},
          currRowIdxInCurrFile{1}, leftOverData{nullptr} {};
    virtual ~ReadFileSharedState() = default;

    virtual void countNumRows() = 0;
    virtual std::unique_ptr<ReadFileMorsel> getMorsel() = 0;
    virtual std::unique_ptr<ReadFileMorsel> getMorselSerial() = 0;

public:
    std::mutex mtx;
    common::row_idx_t numRows;
    catalog::TableSchema* tableSchema;
    std::vector<std::string> filePaths;
    common::CSVReaderConfig csvReaderConfig;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    common::row_idx_t currRowIdx;
    common::block_idx_t currBlockIdx;
    common::vector_idx_t currFileIdx;
    common::row_idx_t currRowIdxInCurrFile;
    common::row_idx_t rowsRead;
    std::shared_ptr<arrow::Table> leftOverData;
};

// For CSV file, we need to read in streaming mode, so we need to keep the reader in the shared
// state.
class ReadCSVSharedState : public ReadFileSharedState {
public:
    ReadCSVSharedState(std::vector<std::string> filePaths, common::CSVReaderConfig csvReaderConfig,
        catalog::TableSchema* tableSchema)
        : ReadFileSharedState{std::move(filePaths), csvReaderConfig, tableSchema} {};

    void countNumRows() final;
    std::unique_ptr<ReadFileMorsel> getMorsel() final;
    std::unique_ptr<ReadFileMorsel> getMorselSerial() final;

private:
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class ReadParquetSharedState : public ReadFileSharedState {
public:
    explicit ReadParquetSharedState(std::vector<std::string> filePaths,
        common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)
        : ReadFileSharedState{std::move(filePaths), csvReaderConfig, tableSchema} {}

private:
    void countNumRows() override;

    std::unique_ptr<storage::ReadFileMorsel> getMorsel() final;
    std::unique_ptr<storage::ReadFileMorsel> getMorselSerial() final;

    std::shared_ptr<parquet::arrow::FileReader> reader;
};

class ReadNPYSharedState : public ReadFileSharedState {
public:
    ReadNPYSharedState(std::vector<std::string> filePaths, common::CSVReaderConfig csvReaderConfig,
        catalog::NodeTableSchema* tableSchema)
        : ReadFileSharedState{std::move(filePaths), csvReaderConfig, tableSchema} {}

private:
    std::unique_ptr<storage::ReadFileMorsel> getMorsel() final;
    std::unique_ptr<storage::ReadFileMorsel> getMorselSerial() final;
    void countNumRows() final;

    std::shared_ptr<storage::NpyMultiFileReader> reader;
};

} // namespace storage
} // namespace kuzu
