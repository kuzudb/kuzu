#pragma once

#include "catalog/catalog.h"
#include "common/copier_config/copier_config.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
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

class TableCopyExecutor {
public:
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
    TableCopyExecutor(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        TablesStatistics* tableStatisticsAndDeletedIDs);

    virtual ~TableCopyExecutor() = default;

    uint64_t copy(processor::ExecutionContext* executionContext);

    static void throwCopyExceptionIfNotOK(const arrow::Status& status);
    static std::unique_ptr<common::Value> getArrowVarList(const std::string& l, int64_t from,
        int64_t to, const common::DataType& dataType,
        const common::CopyDescription& copyDescription);
    static std::unique_ptr<uint8_t[]> getArrowFixedList(const std::string& l, int64_t from,
        int64_t to, const common::DataType& dataType,
        const common::CopyDescription& copyDescription);
    static std::shared_ptr<arrow::csv::StreamingReader> createCSVReader(const std::string& filePath,
        common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<parquet::arrow::FileReader> createParquetReader(
        const std::string& filePath);

protected:
    virtual void initializeColumnsAndLists() = 0;

    virtual void populateColumnsAndLists(processor::ExecutionContext* executionContext) = 0;

    virtual void saveToFile() = 0;

    virtual void populateInMemoryStructures(processor::ExecutionContext* executionContext);

    void countNumLines(const std::vector<std::string>& filePaths);

    void countNumLinesCSV(const std::vector<std::string>& filePaths);

    void countNumLinesParquet(const std::vector<std::string>& filePaths);

    void countNumLinesNpy(const std::vector<std::string>& filePaths);

    static std::vector<std::pair<int64_t, int64_t>> getListElementPos(const std::string& l,
        int64_t from, int64_t to, const common::CopyDescription& copyDescription);

    inline void updateTableStatistics() {
        tablesStatistics->setNumTuplesForTable(tableSchema->tableID, numRows);
    }

    static std::shared_ptr<arrow::DataType> toArrowDataType(const common::DataType& dataType);

private:
    static std::unique_ptr<common::Value> convertStringToValue(std::string element,
        const common::DataType& type, const common::CopyDescription& copyDescription);

protected:
    std::shared_ptr<spdlog::logger> logger;
    common::CopyDescription& copyDescription;
    std::string outputDirectory;
    std::unordered_map<std::string, FileBlockInfo> fileBlockInfos;
    common::TaskScheduler& taskScheduler;
    catalog::Catalog& catalog;
    catalog::TableSchema* tableSchema;
    uint64_t numRows;
    TablesStatistics* tablesStatistics;
};

} // namespace storage
} // namespace kuzu
