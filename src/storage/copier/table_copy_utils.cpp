#include "storage/copier/table_copy_utils.h"

#include "common/constants.h"
#include "common/string_utils.h"
#include "storage/copier/npy_reader.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<CopyMorsel> CopySharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (fileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[fileIdx];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (blockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read in this file.
            fileIdx++;
            blockIdx = 0;
            continue;
        }
        auto result = std::make_unique<CopyMorsel>(
            numTuples, blockIdx, fileBlockInfo.numLinesPerBlock[blockIdx], filePath);
        numTuples += fileBlockInfos.at(filePath).numLinesPerBlock[blockIdx];
        blockIdx++;
        return result;
    }
}

std::unique_ptr<CopyMorsel> CSVCopySharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (fileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[fileIdx];
        if (!reader) {
            reader = TableCopyUtils::createCSVReader(filePath, csvReaderConfig, tableSchema);
        }
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        TableCopyUtils::throwCopyExceptionIfNotOK(reader->ReadNext(&recordBatch));
        if (recordBatch == nullptr) {
            // No more blocks to read in this file.
            fileIdx++;
            reader.reset();
            continue;
        }
        auto morselNodeOffset = numTuples;
        numTuples += recordBatch->num_rows();
        return std::make_unique<CSVCopyMorsel>(morselNodeOffset, filePath, std::move(recordBatch));
    }
}

tuple_idx_t TableCopyUtils::countNumLines(CopyDescription& copyDescription,
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

tuple_idx_t TableCopyUtils::countNumLinesCSV(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    tuple_idx_t numRows = 0;
    // TODO: Count each file as a task.
    for (auto& filePath : copyDescription.filePaths) {
        auto csvStreamingReader =
            createCSVReader(filePath, copyDescription.csvReaderConfig.get(), tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numLinesPerBlock;
        auto startNodeOffset = numRows;
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
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
    }
    return numRows;
}

tuple_idx_t TableCopyUtils::countNumLinesParquet(CopyDescription& copyDescription,
    catalog::TableSchema* tableSchema,
    std::unordered_map<std::string, FileBlockInfo>& fileBlockInfos) {
    tuple_idx_t numRows = 0;
    for (auto& filePath : copyDescription.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader = createParquetReader(filePath);
        auto metadata = reader->parquet_reader()->metadata();
        uint64_t numBlocks = metadata->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        auto startNodeOffset = numRows;
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            numLinesPerBlock[blockIdx] = metadata->RowGroup(blockIdx)->num_rows();
        }
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
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
        auto property = tableSchema->properties[i];
        auto reader = std::make_unique<NpyReader>(filePath);
        auto numNodesInFile = reader->getNumRows();
        if (i == 0) {
            numRows = numNodesInFile;
        }
        reader->validate(property.dataType, numRows, tableSchema->tableName);
        auto numBlocks = (numNodesInFile + CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY - 1) /
                         CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY;
        std::vector<uint64_t> numLinesPerBlock(numBlocks);
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            auto numLines = std::min(CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY,
                numNodesInFile - blockIdx * CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY);
            numLinesPerBlock[blockIdx] = numLines;
        }
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{0 /* start node offset */, numBlocks, numLinesPerBlock});
    }
    return numRows;
}

static bool skipCopyForProperty(const Property& property) {
    return TableSchema::isReservedPropertyName(property.name) ||
           property.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL;
}

std::shared_ptr<arrow::csv::StreamingReader> TableCopyUtils::createCSVReader(
    const std::string& filePath, CSVReaderConfig* csvReaderConfig,
    catalog::TableSchema* tableSchema) {
    std::shared_ptr<arrow::io::InputStream> inputStream;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&inputStream));
    auto csvReadOptions = arrow::csv::ReadOptions::Defaults();
    csvReadOptions.block_size = CopyConstants::CSV_READING_BLOCK_SIZE;
    if (!tableSchema->isNodeTable) {
        csvReadOptions.column_names.emplace_back(Property::REL_FROM_PROPERTY_NAME);
        csvReadOptions.column_names.emplace_back(Property::REL_TO_PROPERTY_NAME);
    }
    for (auto& property : tableSchema->properties) {
        if (skipCopyForProperty(property)) {
            continue;
        }
        csvReadOptions.column_names.push_back(property.name);
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
    for (auto& property : tableSchema->properties) {
        if (property.name == Property::REL_FROM_PROPERTY_NAME ||
            property.name == Property::REL_TO_PROPERTY_NAME) {
            csvConvertOptions.column_types[property.name] = arrow::int64();
            continue;
        }
        if (skipCopyForProperty(property)) {
            continue;
        }
        csvConvertOptions.column_types[property.name] = toArrowDataType(property.dataType);
    }

    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
    throwCopyExceptionIfNotOK(arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
        inputStream, csvReadOptions, csvParseOptions, csvConvertOptions)
                                  .Value(&csvStreamingReader));
    return csvStreamingReader;
}

std::unique_ptr<parquet::arrow::FileReader> TableCopyUtils::createParquetReader(
    const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    throwCopyExceptionIfNotOK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return reader;
}

std::vector<std::pair<int64_t, int64_t>> TableCopyUtils::getListElementPos(
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

std::unique_ptr<Value> TableCopyUtils::getArrowVarList(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
    auto split = getListElementPos(l, from, to, copyDescription);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = VarListType::getChildType(&dataType);
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        auto value = convertStringToValue(element, *childDataType, copyDescription);
        values.push_back(std::move(value));
    }
    auto numBytesOfOverflow =
        values.size() * storage::StorageUtils::getDataTypeSize(*childDataType);
    if (numBytesOfOverflow >= BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(StringUtils::string_format(
            "Maximum num bytes of a LIST is {}. Input list's num bytes is {}.",
            BufferPoolConstants::PAGE_4KB_SIZE, numBytesOfOverflow));
    }
    return make_unique<Value>(
        LogicalType(LogicalTypeID::VAR_LIST,
            std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(*childDataType))),
        std::move(values));
}

std::unique_ptr<uint8_t[]> TableCopyUtils::getArrowFixedList(const std::string& l, int64_t from,
    int64_t to, const LogicalType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto split = getListElementPos(l, from, to, copyDescription);
    auto listVal = std::make_unique<uint8_t[]>(storage::StorageUtils::getDataTypeSize(dataType));
    auto childDataType = FixedListType::getChildType(&dataType);
    uint64_t numElementsRead = 0;
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        switch (childDataType->getLogicalTypeID()) {
        case LogicalTypeID::INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int64_t), &val, sizeof(int64_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::INT32: {
            auto val = TypeUtils::convertStringToNumber<int32_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int32_t), &val, sizeof(int32_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::INT16: {
            auto val = TypeUtils::convertStringToNumber<int16_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int16_t), &val, sizeof(int16_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(double_t), &val, sizeof(double_t));
            numElementsRead++;
        } break;
        case LogicalTypeID::FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(element.c_str());
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
    auto numElementsInList = FixedListType::getNumElementsInList(&dataType);
    if (numElementsRead != numElementsInList) {
        throw CopyException(StringUtils::string_format(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            numElementsInList, numElementsRead));
    }
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
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
    case LogicalTypeID::STRUCT: {
        return arrow::utf8();
    }
    default: {
        throw CopyException("Unsupported data type for CSV " +
                            LogicalTypeUtils::dataTypeToString(dataType.getLogicalTypeID()));
    }
    }
}

std::unique_ptr<Value> TableCopyUtils::convertStringToValue(
    std::string element, const LogicalType& type, const CopyDescription& copyDescription) {
    std::unique_ptr<Value> value;
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int64_t>(element.c_str()));
    } break;
    case LogicalTypeID::INT32: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int32_t>(element.c_str()));
    } break;
    case LogicalTypeID::INT16: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int16_t>(element.c_str()));
    } break;
    case LogicalTypeID::FLOAT: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<float_t>(element.c_str()));
    } break;
    case LogicalTypeID::DOUBLE: {
        value =
            std::make_unique<Value>(TypeUtils::convertStringToNumber<double_t>(element.c_str()));
    } break;
    case LogicalTypeID::BOOL: {
        transform(element.begin(), element.end(), element.begin(), ::tolower);
        std::istringstream is(element);
        bool b;
        is >> std::boolalpha >> b;
        value = std::make_unique<Value>(b);
    } break;
    case LogicalTypeID::STRING: {
        value = make_unique<Value>(LogicalType{LogicalTypeID::STRING}, element);
    } break;
    case LogicalTypeID::DATE: {
        value = std::make_unique<Value>(Date::FromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::TIMESTAMP: {
        value = std::make_unique<Value>(Timestamp::FromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::INTERVAL: {
        value = std::make_unique<Value>(Interval::FromCString(element.c_str(), element.length()));
    } break;
    case LogicalTypeID::VAR_LIST: {
        value = getArrowVarList(element, 1, element.length() - 2, type, copyDescription);
    } break;
    default:
        throw CopyException(
            "Unsupported data type " + LogicalTypeUtils::dataTypeToString(type) + " inside LIST");
    }
    return value;
}

} // namespace storage
} // namespace kuzu
