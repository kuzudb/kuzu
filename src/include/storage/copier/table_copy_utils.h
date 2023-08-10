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
    FileBlockInfo(uint64_t numBlocks, std::vector<uint64_t> numRowsPerBlock)
        : numBlocks{numBlocks}, numRowsPerBlock{std::move(numRowsPerBlock)} {}
    uint64_t numBlocks;
    std::vector<common::row_idx_t> numRowsPerBlock;
};

class TableCopyUtils {
public:
    static void throwCopyExceptionIfNotOK(const arrow::Status& status);
    static std::unique_ptr<common::Value> getVarListValue(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::unique_ptr<common::Value> getArrowFixedListVal(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::unique_ptr<uint8_t[]> getArrowFixedList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::shared_ptr<arrow::csv::StreamingReader> createCSVReader(const std::string& filePath,
        common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema);
    static std::unique_ptr<parquet::arrow::FileReader> createParquetReader(
        const std::string& filePath, catalog::TableSchema* tableSchema);

    static common::row_idx_t countNumLines(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);

    static std::vector<std::pair<int64_t, int64_t>> splitByDelimiter(const std::string& l,
        int64_t from, int64_t to, const common::CopyDescription& copyDescription);

    static std::shared_ptr<arrow::DataType> toArrowDataType(const common::LogicalType& dataType);

    static bool tryCast(const common::LogicalType& targetType, const char* value, uint64_t length);

private:
    static common::row_idx_t countNumLinesCSV(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static common::row_idx_t countNumLinesParquet(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static common::row_idx_t countNumLinesNpy(common::CopyDescription& copyDescription,
        catalog::TableSchema* tableSchema,
        std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos);
    static std::unique_ptr<common::Value> convertStringToValue(std::string element,
        const common::LogicalType& type, const common::CopyDescription& copyDescription);
    static std::vector<std::string> getColumnNamesToRead(catalog::TableSchema* tableSchema);
    static void validateNumElementsInList(
        uint64_t numElementsRead, const common::LogicalType& type);
    static std::unique_ptr<common::Value> parseVarList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CopyDescription& copyDescription);
    static std::unique_ptr<common::Value> parseMap(const std::string& l, int64_t from, int64_t to,
        const common::LogicalType& dataType, const common::CopyDescription& copyDescription);
    static std::pair<std::string, std::string> parseMapFields(const std::string& l, int64_t from,
        int64_t length, const common::CopyDescription& copyDescription);
};

} // namespace storage
} // namespace kuzu
