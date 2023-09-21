#include "storage/store/table_copy_utils.h"

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "storage/storage_structure/lists/lists.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::shared_ptr<arrow::csv::StreamingReader> TableCopyUtils::createRelTableCSVReader(
    const std::string& filePath, const common::ReaderConfig& config) {
    std::shared_ptr<arrow::io::InputStream> inputStream;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&inputStream));
    auto csvReadOptions = arrow::csv::ReadOptions::Defaults();
    csvReadOptions.block_size = CopyConstants::CSV_READING_BLOCK_SIZE;
    for (auto& columnName : config.columnNames) {
        csvReadOptions.column_names.push_back(columnName);
    }
    if (config.csvReaderConfig->hasHeader) {
        csvReadOptions.skip_rows = 1;
    }
    auto csvParseOptions = arrow::csv::ParseOptions::Defaults();
    csvParseOptions.delimiter = config.csvReaderConfig->delimiter;
    csvParseOptions.escape_char = config.csvReaderConfig->escapeChar;
    csvParseOptions.quote_char = config.csvReaderConfig->quoteChar;
    csvParseOptions.ignore_empty_lines = false;
    csvParseOptions.escaping = true;

    auto csvConvertOptions = arrow::csv::ConvertOptions::Defaults();
    csvConvertOptions.strings_can_be_null = true;
    // Only the empty string is treated as NULL.
    csvConvertOptions.null_values = {""};
    csvConvertOptions.quoted_strings_can_be_null = false;
    for (auto i = 0u; i < config.getNumColumns(); ++i) {
        csvConvertOptions.column_types[config.columnNames[i]] =
            toArrowDataType(*config.columnTypes[i]);
    }
    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
    throwCopyExceptionIfNotOK(arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
        inputStream, csvReadOptions, csvParseOptions, csvConvertOptions)
                                  .Value(&csvStreamingReader));
    return csvStreamingReader;
}

std::unique_ptr<parquet::arrow::FileReader> TableCopyUtils::createParquetReader(
    const std::string& filePath, const common::ReaderConfig& config) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    throwCopyExceptionIfNotOK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    auto actualNumColumns =
        reader->parquet_reader()->metadata()->schema()->group_node()->field_count();
    if (config.getNumColumns() != actualNumColumns) {
        // Note: Some parquet files may contain an index column.
        throw CopyException(StringUtils::string_format(
            "Unmatched number of columns in parquet file. Expect: {}, got: {}.",
            config.getNumColumns(), actualNumColumns));
    }
    return reader;
}

std::vector<std::pair<int64_t, int64_t>> TableCopyUtils::splitByDelimiter(
    const std::string& l, int64_t from, int64_t to, const CSVReaderConfig& csvReaderConfig) {
    std::vector<std::pair<int64_t, int64_t>> split;
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    int64_t last = from;
    for (int64_t i = from; i <= to; i++) {
        if (l[i] == csvReaderConfig.listBeginChar) {
            numListBeginChars++;
        } else if (l[i] == csvReaderConfig.listEndChar) {
            numListBeginChars--;
        } else if (l[i] == '{') {
            numStructBeginChars++;
        } else if (l[i] == '}') {
            numStructBeginChars--;
        } else if (l[i] == '\'') {
            numSingleQuotes ^= 1;
        } else if (l[i] == '"') {
            numDoubleQuotes ^= 1;
        } else if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
                   numSingleQuotes == 0 && l[i] == csvReaderConfig.delimiter) {
            split.emplace_back(last, i - last);
            last = i + 1;
        }
    }
    split.emplace_back(last, to - last + 1);
    return split;
}

std::unique_ptr<Value> TableCopyUtils::getVarListValue(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CSVReaderConfig& csvReaderConfig) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::VAR_LIST:
        return parseVarList(l, from, to, dataType, csvReaderConfig);
    case LogicalTypeID::MAP:
        return parseMap(l, from, to, dataType, csvReaderConfig);
    default: {
        throw NotImplementedException{"TableCopyUtils::getVarListValue"};
    }
    }
}

std::unique_ptr<Value> TableCopyUtils::getArrowFixedListVal(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CSVReaderConfig& csvReaderConfig) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto split = splitByDelimiter(l, from, to, csvReaderConfig);
    std::vector<std::unique_ptr<Value>> listValues;
    auto childDataType = FixedListType::getChildType(&dataType);
    uint64_t numElementsRead = 0;
    for (auto [fromPos, len] : split) {
        std::string element = l.substr(fromPos, len);
        if (element.empty()) {
            continue;
        }
        switch (childDataType->getLogicalTypeID()) {
        case LogicalTypeID::INT64:
        case LogicalTypeID::INT32:
        case LogicalTypeID::INT16:
        case LogicalTypeID::INT8:
        case LogicalTypeID::UINT64:
        case LogicalTypeID::UINT32:
        case LogicalTypeID::UINT16:
        case LogicalTypeID::UINT8:
        case LogicalTypeID::DOUBLE:
        case LogicalTypeID::FLOAT: {
            listValues.push_back(convertStringToValue(element, *childDataType, csvReaderConfig));
            numElementsRead++;
        } break;
        default: {
            throw CopyException(
                "Unsupported data type " +
                LogicalTypeUtils::dataTypeToString(*VarListType::getChildType(&dataType)) +
                " inside FIXED_LIST");
        }
        }
    }
    validateNumElementsInList(numElementsRead, dataType);
    return std::make_unique<Value>(dataType, std::move(listValues));
}

std::unique_ptr<uint8_t[]> TableCopyUtils::getArrowFixedList(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CSVReaderConfig& csvReaderConfig) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto split = splitByDelimiter(l, from, to, csvReaderConfig);
    auto listVal = std::make_unique<uint8_t[]>(StorageUtils::getDataTypeSize(dataType));
    auto childDataType = FixedListType::getChildType(&dataType);
    uint64_t numElementsRead = 0;
    for (auto [fromPos, len] : split) {
        std::string element = l.substr(fromPos, len);
        if (element.empty()) {
            continue;
        }
        switch (childDataType->getLogicalTypeID()) {
        case LogicalTypeID::INT64: {
            auto val = StringCastUtils::castToNum<int64_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(int64_t), &val, sizeof(int64_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::INT32: {
            auto val = StringCastUtils::castToNum<int32_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(int32_t), &val, sizeof(int32_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::INT16: {
            auto val = StringCastUtils::castToNum<int16_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(int16_t), &val, sizeof(int16_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::INT8: {
            auto val = StringCastUtils::castToNum<int8_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(int8_t), &val, sizeof(int8_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::UINT64: {
            auto val = StringCastUtils::castToNum<uint64_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(uint64_t), &val, sizeof(uint64_t));
            numElementsRead++;
        }
        case LogicalTypeID::UINT32: {
            auto val = StringCastUtils::castToNum<uint32_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(uint32_t), &val, sizeof(uint32_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::UINT16: {
            auto val = StringCastUtils::castToNum<uint16_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(uint16_t), &val, sizeof(uint16_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::UINT8: {
            auto val = StringCastUtils::castToNum<uint8_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(uint8_t), &val, sizeof(uint8_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::DOUBLE: {
            auto val = StringCastUtils::castToNum<double_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(double_t), &val, sizeof(double_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::FLOAT: {
            auto val = StringCastUtils::castToNum<float_t>(element.c_str(), element.length());
            memcpy(listVal.get() + numElementsRead * sizeof(float_t), &val, sizeof(float_t));
            numElementsRead++;
        } break;
        default: {
            throw CopyException(
                "Unsupported data type " +
                LogicalTypeUtils::dataTypeToString(*VarListType::getChildType(&dataType)) +
                " inside FIXED_LIST");
        }
        }
    }
    validateNumElementsInList(numElementsRead, dataType);
    return listVal;
}

void TableCopyUtils::throwCopyExceptionIfNotOK(const arrow::Status& status) {
    if (!status.ok()) {
        throw CopyException(status.ToString());
    }
}

std::shared_ptr<arrow::DataType> TableCopyUtils::toArrowDataType(const LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return arrow::boolean();
    }
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        return arrow::int64();
    }
    case LogicalTypeID::INT32: {
        return arrow::int32();
    }
    case LogicalTypeID::INT16: {
        return arrow::int16();
    }
    case LogicalTypeID::INT8: {
        return arrow::int8();
    }
    case LogicalTypeID::UINT64: {
        return arrow::uint64();
    }
    case LogicalTypeID::UINT32: {
        return arrow::uint32();
    }
    case LogicalTypeID::UINT16: {
        return arrow::uint16();
    }
    case LogicalTypeID::UINT8: {
        return arrow::uint8();
    }
    case LogicalTypeID::DOUBLE: {
        return arrow::float64();
    }
    case LogicalTypeID::FLOAT: {
        return arrow::float32();
    }
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::DATE:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::UNION: {
        return arrow::utf8();
    }
    default: {
        throw CopyException("Unsupported data type for CSV " +
                            LogicalTypeUtils::dataTypeToString(dataType.getLogicalTypeID()));
    }
    }
}

bool TableCopyUtils::tryCast(
    const common::LogicalType& targetType, const char* value, uint64_t length) {
    switch (targetType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        bool result;
        return StringCastUtils::tryCastToBoolean(value, length, result);
    }
    case LogicalTypeID::INT64: {
        int64_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::INT32: {
        int32_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::INT16: {
        int16_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::INT8: {
        int8_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::UINT64: {
        uint64_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::UINT32: {
        uint32_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::UINT16: {
        uint16_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::UINT8: {
        uint8_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::DOUBLE: {
        double_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::FLOAT: {
        float_t result;
        return StringCastUtils::tryCastToNum(value, length, result);
    }
    case LogicalTypeID::DATE: {
        date_t result;
        uint64_t pos;
        return Date::tryConvertDate(value, length, pos, result);
    }
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t result;
        return Timestamp::tryConvertTimestamp(value, length, result);
    }
    case LogicalTypeID::STRING: {
        return true;
    }
    default: {
        return false;
    }
    }
}

std::vector<StructFieldIdxAndValue> TableCopyUtils::parseStructFieldNameAndValues(
    LogicalType& type, const std::string& structString, const CSVReaderConfig& csvReaderConfig) {
    std::vector<StructFieldIdxAndValue> structFieldIdxAndValueParis;
    uint64_t curPos = 0u;
    while (curPos < structString.length()) {
        auto fieldName = parseStructFieldName(structString, curPos);
        auto fieldIdx = StructType::getFieldIdx(&type, fieldName);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{"Invalid struct field name: " + fieldName};
        }
        auto structFieldValue = parseStructFieldValue(structString, curPos, csvReaderConfig);
        structFieldIdxAndValueParis.emplace_back(fieldIdx, structFieldValue);
    }
    return structFieldIdxAndValueParis;
}

std::unique_ptr<Value> TableCopyUtils::convertStringToValue(
    std::string element, const LogicalType& type, const CSVReaderConfig& csvReaderConfig) {
    std::unique_ptr<Value> value;
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<int64_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::INT32: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<int32_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::INT16: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<int16_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::INT8: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<int8_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::UINT64: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<uint64_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::UINT32: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<uint32_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::UINT16: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<uint16_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::UINT8: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<uint8_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::FLOAT: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<float_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::DOUBLE: {
        value = std::make_unique<Value>(
            StringCastUtils::castToNum<double_t>(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::BOOL: {
        value =
            std::make_unique<Value>(StringCastUtils::castToBool(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::STRING: {
        value = make_unique<Value>(LogicalType{LogicalTypeID::STRING}, element);
    } break;
    case LogicalTypeID::DATE: {
        value = std::make_unique<Value>(Date::fromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::TIMESTAMP: {
        value = std::make_unique<Value>(Timestamp::fromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::INTERVAL: {
        value = std::make_unique<Value>(Interval::fromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::VAR_LIST: {
        value = getVarListValue(element, 1, element.length() - 2, type, csvReaderConfig);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        value = getArrowFixedListVal(element, 1, element.length() - 2, type, csvReaderConfig);
    } break;
    default:
        throw CopyException(
            "Unsupported data type " + LogicalTypeUtils::dataTypeToString(type) + " inside LIST");
    }
    return value;
}

void TableCopyUtils::validateNumElementsInList(uint64_t numElementsRead, const LogicalType& type) {
    auto numElementsInList = FixedListType::getNumElementsInList(&type);
    if (numElementsRead != numElementsInList) {
        throw CopyException(StringUtils::string_format(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            numElementsInList, numElementsRead));
    }
}

std::unique_ptr<Value> TableCopyUtils::parseVarList(const std::string& l, int64_t from, int64_t to,
    const LogicalType& dataType, const CSVReaderConfig& csvReaderConfig) {
    auto split = splitByDelimiter(l, from, to, csvReaderConfig);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = VarListType::getChildType(&dataType);
    // If the list is empty (e.g. []) where from is 1 and to is 0, skip parsing this empty list.
    if (to >= from) {
        for (auto [fromPos, len] : split) {
            std::string element = l.substr(fromPos, len);
            std::unique_ptr<Value> value;
            if (element.empty()) {
                value = std::make_unique<Value>(Value::createNullValue(*childDataType));
            } else {
                value = convertStringToValue(element, *childDataType, csvReaderConfig);
            }
            values.push_back(std::move(value));
        }
    }
    return make_unique<Value>(
        LogicalType(LogicalTypeID::VAR_LIST,
            std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(*childDataType))),
        std::move(values));
}

std::unique_ptr<Value> TableCopyUtils::parseMap(const std::string& l, int64_t from, int64_t to,
    const LogicalType& dataType, const CSVReaderConfig& csvReaderConfig) {
    auto split = splitByDelimiter(l, from, to, csvReaderConfig);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = VarListType::getChildType(&dataType);
    auto structFieldTypes = StructType::getFieldTypes(childDataType);
    // If the map is empty (e.g. {}) where from is 1 and to is 0, skip parsing this empty list.
    if (to >= from) {
        for (auto [fromPos, len] : split) {
            std::vector<std::unique_ptr<Value>> structFields{2};
            auto [key, value] = parseMapFields(l, fromPos, len, csvReaderConfig);
            // TODO(Ziyi): refactor the convertStringToValue API so we can reuse the value during
            // parsing.
            structFields[0] = convertStringToValue(key, *structFieldTypes[0], csvReaderConfig);
            structFields[1] = convertStringToValue(value, *structFieldTypes[1], csvReaderConfig);
            values.push_back(std::make_unique<Value>(
                *VarListType::getChildType(&dataType), std::move(structFields)));
        }
    }
    return make_unique<Value>(dataType, std::move(values));
}

std::pair<std::string, std::string> TableCopyUtils::parseMapFields(
    const std::string& l, int64_t from, int64_t length, const CSVReaderConfig& csvReaderConfig) {
    std::string key;
    std::string value;
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    for (auto i = 0u; i < length; i++) {
        auto curPos = i + from;
        auto curChar = l[curPos];
        if (curChar == '{') {
            numStructBeginChars++;
        } else if (curChar == '}') {
            numStructBeginChars--;
        } else if (curChar == csvReaderConfig.listBeginChar) {
            numListBeginChars++;
        } else if (curChar == csvReaderConfig.listEndChar) {
            numListBeginChars--;
        } else if (curChar == '"') {
            numDoubleQuotes ^= 1;
        } else if (curChar == '\'') {
            numSingleQuotes ^= 1;
        } else if (curChar == '=') {
            if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
                numSingleQuotes == 0) {
                return {l.substr(from, i), l.substr(curPos + 1, length - i - 1)};
            }
        }
    }
    throw ParserException{
        StringUtils::string_format("Invalid map field string {}.", l.substr(from, length))};
}

std::unique_ptr<arrow::PrimitiveArray> TableCopyUtils::createArrowPrimitiveArray(
    const std::shared_ptr<arrow::DataType>& type, const uint8_t* data, uint64_t length) {
    auto buffer = std::make_shared<arrow::Buffer>(data, length);
    return std::make_unique<arrow::PrimitiveArray>(type, length, buffer);
}

std::unique_ptr<arrow::PrimitiveArray> TableCopyUtils::createArrowPrimitiveArray(
    const std::shared_ptr<arrow::DataType>& type, std::shared_ptr<arrow::Buffer> buffer,
    uint64_t length) {
    return std::make_unique<arrow::PrimitiveArray>(type, length, buffer);
}

std::string TableCopyUtils::parseStructFieldName(
    const std::string& structString, uint64_t& curPos) {
    auto startPos = curPos;
    while (curPos < structString.length()) {
        if (structString[curPos] == ':') {
            auto structFieldName = structString.substr(startPos, curPos - startPos);
            StringUtils::removeWhiteSpaces(structFieldName);
            curPos++;
            return structFieldName;
        }
        curPos++;
    }
    throw ParserException{"Invalid struct string: " + structString};
}

std::string TableCopyUtils::parseStructFieldValue(
    const std::string& structString, uint64_t& curPos, const CSVReaderConfig& csvReaderConfig) {
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    // Skip leading white spaces.
    while (structString[curPos] == ' ') {
        curPos++;
    }
    auto startPos = curPos;
    while (curPos < structString.length()) {
        auto curChar = structString[curPos];
        if (curChar == '{') {
            numStructBeginChars++;
        } else if (curChar == '}') {
            numStructBeginChars--;
        } else if (curChar == csvReaderConfig.listBeginChar) {
            numListBeginChars++;
        } else if (curChar == csvReaderConfig.listEndChar) {
            numListBeginChars--;
        } else if (curChar == '"') {
            numDoubleQuotes ^= 1;
        } else if (curChar == '\'') {
            numSingleQuotes ^= 1;
        } else if (curChar == ',') {
            if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
                numSingleQuotes == 0) {
                curPos++;
                return structString.substr(startPos, curPos - startPos - 1);
            }
        }
        curPos++;
    }
    if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
        numSingleQuotes == 0) {
        return structString.substr(startPos, curPos - startPos);
    } else {
        throw ParserException{"Invalid struct string: " + structString};
    }
}

} // namespace storage
} // namespace kuzu
