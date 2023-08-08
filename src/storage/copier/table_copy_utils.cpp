#include "storage/copier/table_copy_utils.h"

#include "common/constants.h"
#include "common/string_utils.h"
#include "storage/copier/npy_reader.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t TableCopyUtils::countNumLines(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        return countNumLinesCSV(copyDescription, tableSchema, fileBlockInfos);
    }
    case CopyDescription::FileType::PARQUET: {
        return countNumLinesParquet(copyDescription, tableSchema, fileBlockInfos);
    }
    case CopyDescription::FileType::NPY: {
        return countNumLinesNpy(copyDescription, tableSchema, fileBlockInfos);
    }
    default: {
        throw CopyException{StringUtils::string_format("Unrecognized file type: {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType))};
    }
    }
}

row_idx_t TableCopyUtils::countNumLinesCSV(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    row_idx_t numRows = 0;
    // TODO: Count each file as a task.
    for (auto& filePath : copyDescription.filePaths) {
        auto csvStreamingReader =
            createCSVReader(filePath, copyDescription.csvReaderConfig.get(), tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numLinesPerBlock;
        while (true) {
            throwCopyExceptionIfNotOK(csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == nullptr) {
                break;
            }
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numRows += currNumRows;
        }
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numLinesPerBlock});
    }
    return numRows;
}

row_idx_t TableCopyUtils::countNumLinesParquet(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    row_idx_t numRows = 0;
    for (auto& filePath : copyDescription.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            createParquetReader(filePath, tableSchema);
        auto metadata = reader->parquet_reader()->metadata();
        uint64_t numBlocks = metadata->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            numLinesPerBlock[blockIdx] = metadata->RowGroup(blockIdx)->num_rows();
        }
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numLinesPerBlock});
        numRows += metadata->num_rows();
    }
    return numRows;
}

offset_t TableCopyUtils::countNumLinesNpy(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    offset_t numRows = 0;
    for (auto i = 0u; i < copyDescription.filePaths.size(); i++) {
        auto filePath = copyDescription.filePaths[i];
        auto property = tableSchema->properties[i].get();
        auto reader = std::make_unique<NpyReader>(filePath);
        auto numNodesInFile = reader->getNumRows();
        if (i == 0) {
            numRows = numNodesInFile;
        }
        reader->validate(*property->getDataType(), numRows, tableSchema->tableName);
        auto numBlocks = (numNodesInFile + CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY - 1) /
                         CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY;
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            auto numLines = std::min(CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY,
                numNodesInFile - blockIdx * CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
            numLinesPerBlock[blockIdx] = numLines;
        }
        fileBlockInfos.emplace(filePath, FileBlockInfo{numBlocks, numLinesPerBlock});
    }
    return numRows;
}

static bool skipCopyForProperty(const Property& property) {
    return TableSchema::isReservedPropertyName(property.getName()) ||
           property.getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL;
}

std::shared_ptr<arrow::csv::StreamingReader> TableCopyUtils::createCSVReader(
    const std::string& filePath, CSVReaderConfig* csvReaderConfig,
    catalog::TableSchema* tableSchema) {
    std::shared_ptr<arrow::io::InputStream> inputStream;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&inputStream));
    auto csvReadOptions = arrow::csv::ReadOptions::Defaults();
    csvReadOptions.block_size = CopyConstants::CSV_READING_BLOCK_SIZE;
    for (auto& columnName : getColumnNamesToRead(tableSchema)) {
        csvReadOptions.column_names.push_back(columnName);
    }
    if (csvReaderConfig->hasHeader) {
        csvReadOptions.skip_rows = 1;
    }
    auto csvParseOptions = arrow::csv::ParseOptions::Defaults();
    csvParseOptions.delimiter = csvReaderConfig->delimiter;
    csvParseOptions.escape_char = csvReaderConfig->escapeChar;
    csvParseOptions.quote_char = csvReaderConfig->quoteChar;
    csvParseOptions.ignore_empty_lines = false;
    csvParseOptions.escaping = true;

    auto csvConvertOptions = arrow::csv::ConvertOptions::Defaults();
    csvConvertOptions.strings_can_be_null = true;
    // Only the empty string is treated as NULL.
    csvConvertOptions.null_values = {""};
    csvConvertOptions.quoted_strings_can_be_null = false;
    if (tableSchema->tableType == TableType::REL) {
        auto relTableSchema = (RelTableSchema*)tableSchema;
        csvConvertOptions.column_types[std::string(Property::REL_FROM_PROPERTY_NAME)] =
            toArrowDataType(*relTableSchema->getSrcPKDataType());
        csvConvertOptions.column_types[std::string(Property::REL_TO_PROPERTY_NAME)] =
            toArrowDataType(*relTableSchema->getDstPKDataType());
    }
    for (auto& property : tableSchema->properties) {
        if (property->getName() == Property::REL_FROM_PROPERTY_NAME ||
            property->getName() == Property::REL_TO_PROPERTY_NAME) {
            csvConvertOptions.column_types[property->getName()] = arrow::int64();
            continue;
        }
        if (skipCopyForProperty(*property)) {
            continue;
        }
        csvConvertOptions.column_types[property->getName()] =
            toArrowDataType(*property->getDataType());
    }

    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
    throwCopyExceptionIfNotOK(arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
        inputStream, csvReadOptions, csvParseOptions, csvConvertOptions)
                                  .Value(&csvStreamingReader));
    return csvStreamingReader;
}

std::unique_ptr<parquet::arrow::FileReader> TableCopyUtils::createParquetReader(
    const std::string& filePath, TableSchema* tableSchema) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    throwCopyExceptionIfNotOK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    auto expectedNumColumns = getColumnNamesToRead(tableSchema).size();
    auto actualNumColumns =
        reader->parquet_reader()->metadata()->schema()->group_node()->field_count();
    if (expectedNumColumns != actualNumColumns) {
        // Note: Some parquet files may contain an index column.
        throw CopyException(StringUtils::string_format(
            "Unmatched number of columns in parquet file. Expect: {}, got: {}.", expectedNumColumns,
            actualNumColumns));
    }
    return reader;
}

std::vector<std::pair<int64_t, int64_t>> TableCopyUtils::splitByDelimiter(
    const std::string& l, int64_t from, int64_t to, const CopyDescription& copyDescription) {
    std::vector<std::pair<int64_t, int64_t>> split;
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    int64_t last = from;
    for (int64_t i = from; i <= to; i++) {
        if (l[i] == copyDescription.csvReaderConfig->listBeginChar) {
            numListBeginChars++;
        } else if (l[i] == copyDescription.csvReaderConfig->listEndChar) {
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
                   numSingleQuotes == 0 && l[i] == copyDescription.csvReaderConfig->delimiter) {
            split.emplace_back(last, i - last);
            last = i + 1;
        }
    }
    split.emplace_back(last, to - last + 1);
    return split;
}

std::unique_ptr<Value> TableCopyUtils::getVarListValue(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CopyDescription& copyDescription) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::VAR_LIST:
        return parseVarList(l, from, to, dataType, copyDescription);
    case LogicalTypeID::MAP:
        return parseMap(l, from, to, dataType, copyDescription);
    default: {
        throw NotImplementedException{"TableCopyUtils::getVarListValue"};
    }
    }
}

std::unique_ptr<Value> TableCopyUtils::getArrowFixedListVal(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto split = splitByDelimiter(l, from, to, copyDescription);
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
        case LogicalTypeID::DOUBLE:
        case LogicalTypeID::FLOAT: {
            listValues.push_back(convertStringToValue(element, *childDataType, copyDescription));
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
    int64_t to, const LogicalType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto split = splitByDelimiter(l, from, to, copyDescription);
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

std::unique_ptr<Value> TableCopyUtils::convertStringToValue(
    std::string element, const LogicalType& type, const CopyDescription& copyDescription) {
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
        value = getVarListValue(element, 1, element.length() - 2, type, copyDescription);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        value = getArrowFixedListVal(element, 1, element.length() - 2, type, copyDescription);
    } break;
    default:
        throw CopyException(
            "Unsupported data type " + LogicalTypeUtils::dataTypeToString(type) + " inside LIST");
    }
    return value;
}

std::vector<std::string> TableCopyUtils::getColumnNamesToRead(catalog::TableSchema* tableSchema) {
    std::vector<std::string> columnNamesToRead;
    if (tableSchema->tableType == TableType::REL) {
        columnNamesToRead.emplace_back(Property::REL_FROM_PROPERTY_NAME);
        columnNamesToRead.emplace_back(Property::REL_TO_PROPERTY_NAME);
    }
    for (auto& property : tableSchema->properties) {
        if (skipCopyForProperty(*property)) {
            continue;
        }
        columnNamesToRead.push_back(property->getName());
    }
    return columnNamesToRead;
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
    const LogicalType& dataType, const CopyDescription& copyDescription) {
    auto split = splitByDelimiter(l, from, to, copyDescription);
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
                value = convertStringToValue(element, *childDataType, copyDescription);
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
    const LogicalType& dataType, const CopyDescription& copyDescription) {
    auto split = splitByDelimiter(l, from, to, copyDescription);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = VarListType::getChildType(&dataType);
    auto structFieldTypes = StructType::getFieldTypes(childDataType);
    // If the map is empty (e.g. {}) where from is 1 and to is 0, skip parsing this empty list.
    if (to >= from) {
        for (auto [fromPos, len] : split) {
            std::vector<std::unique_ptr<Value>> structFields{2};
            auto [key, value] = parseMapFields(l, fromPos, len, copyDescription);
            // TODO(Ziyi): refactor the convertStringToValue API so we can reuse the value during
            // parsing.
            structFields[0] = convertStringToValue(key, *structFieldTypes[0], copyDescription);
            structFields[1] = convertStringToValue(value, *structFieldTypes[1], copyDescription);
            values.push_back(std::make_unique<Value>(
                *VarListType::getChildType(&dataType), std::move(structFields)));
        }
    }
    return make_unique<Value>(dataType, std::move(values));
}

std::pair<std::string, std::string> TableCopyUtils::parseMapFields(
    const std::string& l, int64_t from, int64_t length, const CopyDescription& copyDescription) {
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
        } else if (curChar == copyDescription.csvReaderConfig->listBeginChar) {
            numListBeginChars++;
        } else if (curChar == copyDescription.csvReaderConfig->listEndChar) {
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

} // namespace storage
} // namespace kuzu
