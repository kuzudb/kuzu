#pragma once

#include "catalog/catalog.h"
#include "common/copier_config/copier_config.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

class TableCopier {
    struct FileBlockInfo {
        FileBlockInfo(common::offset_t startOffset, uint64_t numBlocks,
            std::vector<uint64_t> numLinesPerBlock)
            : startOffset{startOffset}, numBlocks{numBlocks}, numLinesPerBlock{
                                                                  std::move(numLinesPerBlock)} {}
        common::offset_t startOffset;
        uint64_t numBlocks;
        std::vector<uint64_t> numLinesPerBlock;
    };

public:
    TableCopier(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog,
        common::table_id_t tableID);

    virtual ~TableCopier() = default;

    uint64_t copy();

protected:
    virtual void updateTableStatistics() = 0;

    virtual void initializeColumnsAndLists() = 0;

    virtual void populateColumnsAndLists() = 0;

    virtual void saveToFile() = 0;

    virtual void populateInMemoryStructures();

    void countNumLines(const std::vector<std::string>& filePath);

    arrow::Status countNumLinesCSV(const std::vector<std::string>& filePaths);

    arrow::Status countNumLinesParquet(const std::vector<std::string>& filePaths);

    arrow::Status initCSVReaderAndCheckStatus(
        std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
        const std::string& filePath);
    arrow::Status initCSVReader(std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
        const std::string& filePath);

    arrow::Status initArrowReaderAndCheckStatus(
        std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader,
        const std::string& filePath);
    arrow::Status initArrowReader(std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader,
        const std::string& filePath);

    arrow::Status initParquetReaderAndCheckStatus(
        std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath);

    arrow::Status initParquetReader(
        std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath);

    static std::vector<std::pair<int64_t, int64_t>> getListElementPos(
        std::string& l, int64_t from, int64_t to, common::CopyDescription& copyDescription);

    static std::unique_ptr<common::Value> getArrowVarList(std::string& l, int64_t from, int64_t to,
        const common::DataType& dataType, common::CopyDescription& copyDescription);

    static std::unique_ptr<uint8_t[]> getArrowFixedList(std::string& l, int64_t from, int64_t to,
        const common::DataType& dataType, common::CopyDescription& copyDescription);

    static void throwCopyExceptionIfNotOK(const arrow::Status& status);

    uint64_t getNumBlocks() const {
        uint64_t numBlocks = 0;
        for (auto& [_, info] : fileBlockInfos) {
            numBlocks += info.numBlocks;
        }
        return numBlocks;
    }

protected:
    std::shared_ptr<spdlog::logger> logger;
    common::CopyDescription& copyDescription;
    std::string outputDirectory;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    common::TaskScheduler& taskScheduler;
    catalog::Catalog& catalog;
    catalog::TableSchema* tableSchema;
    uint64_t numRows;
};

} // namespace storage
} // namespace kuzu
