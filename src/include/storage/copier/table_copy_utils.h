#pragma once

#include "catalog/catalog.h"
#include "common/copier_config/copier_config.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "storage/store/table_statistics.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

struct FileBlockInfo {
    FileBlockInfo(
        common::offset_t startOffset, uint64_t numBlocks, std::vector<uint64_t> numLinesPerBlock)
        : startOffset{startOffset}, numBlocks{numBlocks}, numLinesPerBlock{
                                                              std::move(numLinesPerBlock)} {}
    common::offset_t startOffset;
    uint64_t numBlocks;
    std::vector<uint64_t> numLinesPerBlock;
};

class CopyMorsel {
public:
    static constexpr common::block_idx_t BLOCK_IDX_INVALID = UINT64_MAX;

    CopyMorsel(common::tuple_idx_t tupleIdx, common::block_idx_t blockIdx,
        common::tuple_idx_t numTuples, std::string filePath)
        : tupleIdx{tupleIdx}, blockIdx{blockIdx}, numTuples{numTuples}, filePath{
                                                                            std::move(filePath)} {};
    virtual ~CopyMorsel() = default;

public:
    common::tuple_idx_t tupleIdx;
    common::block_idx_t blockIdx;
    uint64_t numTuples;
    std::string filePath;
};

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class CSVCopyMorsel : public CopyMorsel {
public:
    CSVCopyMorsel(common::offset_t startOffset, std::string filePath,
        std::shared_ptr<arrow::RecordBatch> recordBatch)
        : CopyMorsel{startOffset, BLOCK_IDX_INVALID, UINT64_MAX, std::move(filePath)},
          recordBatch{std::move(recordBatch)} {}

    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class CopySharedState {
public:
    CopySharedState(std::vector<std::string> filePaths,
        std::unordered_map<std::string, FileBlockInfo> fileBlockInfos)
        : filePaths{std::move(filePaths)}, fileIdx{0}, blockIdx{0}, numTuples{0},
          fileBlockInfos{std::move(fileBlockInfos)} {};
    virtual ~CopySharedState() = default;

    virtual std::unique_ptr<CopyMorsel> getMorsel();

public:
    std::vector<std::string> filePaths;
    common::field_idx_t fileIdx;
    common::tuple_idx_t numTuples;

protected:
    std::mutex mtx;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    common::block_idx_t blockIdx;
};

// For CSV file, we need to read in streaming mode, so we need to keep the reader in the shared
// state.
class CSVCopySharedState : public CopySharedState {
public:
    CSVCopySharedState(std::vector<std::string> filePaths,
        std::unordered_map<std::string, FileBlockInfo> fileBlockInfos,
        common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema)
        : CopySharedState{std::move(filePaths), std::move(fileBlockInfos)},
          csvReaderConfig{csvReaderConfig}, tableSchema{tableSchema} {};

    std::unique_ptr<CopyMorsel> getMorsel() final;

private:
    common::CSVReaderConfig* csvReaderConfig;
    catalog::TableSchema* tableSchema;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class TableCopyUtils {
public:
    static void throwCopyExceptionIfNotOK(const arrow::Status& status);
    static std::unique_ptr<common::Value> getArrowVarList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::unique_ptr<uint8_t[]> getArrowFixedList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::shared_ptr<arrow::csv::StreamingReader> createCSVReader(const std::string& filePath,
        common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<parquet::arrow::FileReader> createParquetReader(
        const std::string& filePath);

    static common::tuple_idx_t countNumLines(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);

    static std::vector<std::pair<int64_t, int64_t>> getListElementPos(const std::string& l,
        int64_t from, int64_t to, const common::CopyDescription& copyDescription);

    static std::shared_ptr<arrow::DataType> toArrowDataType(const common::LogicalType& dataType);

private:
    static common::tuple_idx_t countNumLinesCSV(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static common::tuple_idx_t countNumLinesParquet(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static common::tuple_idx_t countNumLinesNpy(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static std::unique_ptr<common::Value> convertStringToValue(std::string element,
        const common::LogicalType& type, const common::CopyDescription& copyDescription);
};

} // namespace storage
} // namespace kuzu
