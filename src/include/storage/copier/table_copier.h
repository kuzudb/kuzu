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
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        TablesStatistics* tableStatisticsAndDeletedIDs);

    virtual ~TableCopier() = default;

    uint64_t copy(processor::ExecutionContext* executionContext);

protected:
    virtual void initializeColumnsAndLists() = 0;

    virtual void populateColumnsAndLists(processor::ExecutionContext* executionContext) = 0;

    virtual void saveToFile() = 0;

    virtual void populateInMemoryStructures(processor::ExecutionContext* executionContext);

    void countNumLines(const std::vector<std::string>& filePaths);

    void countNumLinesCSV(const std::vector<std::string>& filePaths);

    void countNumLinesParquet(const std::vector<std::string>& filePaths);

    std::shared_ptr<arrow::csv::StreamingReader> initCSVReader(const std::string& filePath) const;

    std::unique_ptr<parquet::arrow::FileReader> initParquetReader(const std::string& filePath);

    static std::vector<std::pair<int64_t, int64_t>> getListElementPos(
        const std::string& l, int64_t from, int64_t to, common::CopyDescription& copyDescription);

    static std::unique_ptr<common::Value> getArrowVarList(const std::string& l, int64_t from,
        int64_t to, const common::DataType& dataType, common::CopyDescription& copyDescription);

    static std::unique_ptr<uint8_t[]> getArrowFixedList(const std::string& l, int64_t from,
        int64_t to, const common::DataType& dataType, common::CopyDescription& copyDescription);

    static void throwCopyExceptionIfNotOK(const arrow::Status& status);

    inline void updateTableStatistics() {
        tablesStatistics->setNumTuplesForTable(tableSchema->tableID, numRows);
    }
    inline uint64_t getNumBlocks() const {
        uint64_t numBlocks = 0;
        for (auto& [_, info] : fileBlockInfos) {
            numBlocks += info.numBlocks;
        }
        return numBlocks;
    }

    static std::shared_ptr<arrow::DataType> toArrowDataType(const common::DataType& dataType);

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
