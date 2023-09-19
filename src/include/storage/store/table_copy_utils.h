#pragma once

#include "common/copier_config/copier_config.h"

namespace arrow {
class Buffer;
class DataType;
class PrimitiveArray;
class Status;
namespace csv {
class StreamingReader;
}
} // namespace arrow
namespace parquet {
namespace arrow {
class FileReader;
}
} // namespace parquet

namespace kuzu {
namespace common {
class Value;
}

namespace storage {

struct StructFieldIdxAndValue {
    StructFieldIdxAndValue(common::struct_field_idx_t fieldIdx, std::string fieldValue)
        : fieldIdx{fieldIdx}, fieldValue{std::move(fieldValue)} {}

    common::struct_field_idx_t fieldIdx;
    std::string fieldValue;
};

class TableCopyUtils {
public:
    static void throwCopyExceptionIfNotOK(const arrow::Status& status);
    static std::unique_ptr<common::Value> getVarListValue(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CSVReaderConfig& csvReaderConfig);
    static std::unique_ptr<common::Value> getArrowFixedListVal(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CSVReaderConfig& csvReaderConfig);
    static std::unique_ptr<uint8_t[]> getArrowFixedList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CSVReaderConfig& csvReaderConfig);
    static std::shared_ptr<arrow::csv::StreamingReader> createRelTableCSVReader(
        const std::string& filePath, const common::ReaderConfig& config);
    static std::unique_ptr<parquet::arrow::FileReader> createParquetReader(
        const std::string& filePath, const common::ReaderConfig& config);

    static std::vector<std::pair<int64_t, int64_t>> splitByDelimiter(const std::string& l,
        int64_t from, int64_t to, const common::CSVReaderConfig& csvReaderConfig);

    static std::shared_ptr<arrow::DataType> toArrowDataType(const common::LogicalType& dataType);

    static bool tryCast(const common::LogicalType& targetType, const char* value, uint64_t length);

    static std::vector<StructFieldIdxAndValue> parseStructFieldNameAndValues(
        common::LogicalType& type, const std::string& structString,
        const common::CSVReaderConfig& csvReaderConfig);

    static std::unique_ptr<arrow::PrimitiveArray> createArrowPrimitiveArray(
        const std::shared_ptr<arrow::DataType>& type, const uint8_t* data, uint64_t length);
    static std::unique_ptr<arrow::PrimitiveArray> createArrowPrimitiveArray(
        const std::shared_ptr<arrow::DataType>& type, std::shared_ptr<arrow::Buffer> buffer,
        uint64_t length);

private:
    static std::unique_ptr<common::Value> convertStringToValue(std::string element,
        const common::LogicalType& type, const common::CSVReaderConfig& csvReaderConfig);

    static void validateNumElementsInList(
        uint64_t numElementsRead, const common::LogicalType& type);
    static std::unique_ptr<common::Value> parseVarList(const std::string& l, int64_t from,
        int64_t to, const common::LogicalType& dataType,
        const common::CSVReaderConfig& csvReaderConfig);
    static std::unique_ptr<common::Value> parseMap(const std::string& l, int64_t from, int64_t to,
        const common::LogicalType& dataType, const common::CSVReaderConfig& csvReaderConfig);
    static std::pair<std::string, std::string> parseMapFields(const std::string& l, int64_t from,
        int64_t length, const common::CSVReaderConfig& csvReaderConfig);
    static std::string parseStructFieldName(const std::string& structString, uint64_t& curPos);
    static std::string parseStructFieldValue(const std::string& structString, uint64_t& curPos,
        const common::CSVReaderConfig& csvReaderConfig);
};

} // namespace storage
} // namespace kuzu
