#include "storage/copier/table_copy_executor.h"

#include "common/constants.h"
#include "common/string_utils.h"
#include "storage/copier/npy_reader.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

TableCopyExecutor::TableCopyExecutor(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, common::table_id_t tableID,
    TablesStatistics* tablesStatistics)
    : logger{LoggerUtils::getLogger(LoggerConstants::LoggerEnum::LOADER)},
      copyDescription{copyDescription}, outputDirectory{std::move(outputDirectory)},
      taskScheduler{taskScheduler}, catalog{catalog}, numRows{0},
      tableSchema{catalog.getReadOnlyVersion()->getTableSchema(tableID)}, tablesStatistics{
                                                                              tablesStatistics} {}

uint64_t TableCopyExecutor::copy(processor::ExecutionContext* executionContext) {
    logger->info(StringUtils::string_format("Copying {} file to table {}.",
        CopyDescription::getFileTypeName(copyDescription.fileType), tableSchema->tableName));
    populateInMemoryStructures(executionContext);
    updateTableStatistics();
    saveToFile();
    logger->info("Done copying file to table {}.", tableSchema->tableName);
    return numRows;
}

void TableCopyExecutor::populateInMemoryStructures(processor::ExecutionContext* executionContext) {
    countNumLines(copyDescription.filePaths);
    initializeColumnsAndLists();
    populateColumnsAndLists(executionContext);
}

void TableCopyExecutor::countNumLines(const std::vector<std::string>& filePaths) {
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        countNumLinesCSV(filePaths);
    } break;
    case CopyDescription::FileType::PARQUET: {
        countNumLinesParquet(filePaths);
    } break;
    case CopyDescription::FileType::NPY: {
        countNumLinesNpy(filePaths);
    } break;
    default: {
        throw CopyException{StringUtils::string_format("Unrecognized file type: {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType))};
    }
    }
}

void TableCopyExecutor::countNumLinesCSV(const std::vector<std::string>& filePaths) {
    numRows = 0;
    for (auto& filePath : filePaths) {
        auto csvStreamingReader =
            createCSVReader(filePath, copyDescription.csvReaderConfig.get(), tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numLinesPerBlock;
        auto startNodeOffset = numRows;
        while (true) {
            throwCopyExceptionIfNotOK(csvStreamingReader->ReadNext(&currBatch));
            if (currBatch == NULL) {
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
}

void TableCopyExecutor::countNumLinesParquet(const std::vector<std::string>& filePaths) {
    numRows = 0;
    for (auto& filePath : filePaths) {
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
}

void TableCopyExecutor::countNumLinesNpy(const std::vector<std::string>& filePaths) {
    numRows = 0;
    for (auto i = 0u; i < filePaths.size(); i++) {
        auto filePath = filePaths[i];
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
}

static bool skipCopyForProperty(const Property& property) {
    return TableSchema::isReservedPropertyName(property.name) || property.dataType.typeID == SERIAL;
}

std::shared_ptr<arrow::csv::StreamingReader> TableCopyExecutor::createCSVReader(
    const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
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

std::unique_ptr<parquet::arrow::FileReader> TableCopyExecutor::createParquetReader(
    const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    throwCopyExceptionIfNotOK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return reader;
}

std::vector<std::pair<int64_t, int64_t>> TableCopyExecutor::getListElementPos(
    const std::string& l, int64_t from, int64_t to, const CopyDescription& copyDescription) {
    std::vector<std::pair<int64_t, int64_t>> split;
    int bracket = 0;
    int64_t last = from;
    for (int64_t i = from; i <= to; i++) {
        if (l[i] == copyDescription.csvReaderConfig->listBeginChar) {
            bracket += 1;
        } else if (l[i] == copyDescription.csvReaderConfig->listEndChar) {
            bracket -= 1;
        } else if (bracket == 0 && l[i] == copyDescription.csvReaderConfig->delimiter) {
            split.emplace_back(last, i - last);
            last = i + 1;
        }
    }
    split.emplace_back(last, to - last + 1);
    return split;
}

std::unique_ptr<Value> TableCopyExecutor::getArrowVarList(const std::string& l, int64_t from,
    int64_t to, const DataType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.typeID == common::VAR_LIST || dataType.typeID == common::FIXED_LIST);
    auto split = getListElementPos(l, from, to, copyDescription);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = *dataType.getChildType();
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        auto value = convertStringToValue(element, *dataType.getChildType(), copyDescription);
        values.push_back(std::move(value));
    }
    auto numBytesOfOverflow = values.size() * Types::getDataTypeSize(childDataType.typeID);
    if (numBytesOfOverflow >= BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(StringUtils::string_format(
            "Maximum num bytes of a LIST is {}. Input list's num bytes is {}.",
            BufferPoolConstants::PAGE_4KB_SIZE, numBytesOfOverflow));
    }
    return make_unique<Value>(
        DataType(std::make_unique<DataType>(childDataType)), std::move(values));
}

std::unique_ptr<uint8_t[]> TableCopyExecutor::getArrowFixedList(const std::string& l, int64_t from,
    int64_t to, const DataType& dataType, const CopyDescription& copyDescription) {
    assert(dataType.typeID == common::FIXED_LIST);
    auto split = getListElementPos(l, from, to, copyDescription);
    auto listVal = std::make_unique<uint8_t[]>(Types::getDataTypeSize(dataType));
    auto childDataType = *dataType.getChildType();
    uint64_t numElementsRead = 0;
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        switch (childDataType.typeID) {
        case INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int64_t), &val, sizeof(int64_t));
            numElementsRead++;
        } break;
        case INT32: {
            auto val = TypeUtils::convertStringToNumber<int32_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int32_t), &val, sizeof(int32_t));
            numElementsRead++;
        } break;
        case INT16: {
            auto val = TypeUtils::convertStringToNumber<int16_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(int16_t), &val, sizeof(int16_t));
            numElementsRead++;
        } break;
        case DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(double_t), &val, sizeof(double_t));
            numElementsRead++;
        } break;
        case FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(element.c_str());
            memcpy(listVal.get() + numElementsRead * sizeof(float_t), &val, sizeof(float_t));
            numElementsRead++;
        } break;
        default: {
            throw CopyException("Unsupported data type " +
                                Types::dataTypeToString(dataType.getChildType()->typeID) +
                                " inside FIXED_LIST");
        }
        }
    }
    auto extraTypeInfo = reinterpret_cast<FixedListTypeInfo*>(dataType.getExtraTypeInfo());
    if (numElementsRead != extraTypeInfo->getFixedNumElementsInList()) {
        throw CopyException(StringUtils::string_format(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            extraTypeInfo->getFixedNumElementsInList(), numElementsRead));
    }
    return listVal;
}

void TableCopyExecutor::throwCopyExceptionIfNotOK(const arrow::Status& status) {
    if (!status.ok()) {
        throw CopyException(status.ToString());
    }
}

std::shared_ptr<arrow::DataType> TableCopyExecutor::toArrowDataType(
    const common::DataType& dataType) {
    switch (dataType.typeID) {
    case common::BOOL: {
        return arrow::boolean();
    }
    case common::INT64: {
        return arrow::int64();
    }
    case common::INT32: {
        return arrow::int32();
    }
    case common::INT16: {
        return arrow::int16();
    }
    case common::DOUBLE: {
        return arrow::float64();
    }
    case common::FLOAT: {
        return arrow::float32();
    }
    case common::TIMESTAMP:
    case common::DATE:
    case common::INTERVAL:
    case common::FIXED_LIST:
    case common::VAR_LIST:
    case common::STRING:
    case common::STRUCT: {
        return arrow::utf8();
    }
    default: {
        throw CopyException(
            "Unsupported data type for CSV " + Types::dataTypeToString(dataType.typeID));
    }
    }
}

std::unique_ptr<Value> TableCopyExecutor::convertStringToValue(
    std::string element, const DataType& type, const CopyDescription& copyDescription) {
    std::unique_ptr<Value> value;
    switch (type.typeID) {
    case INT64: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int64_t>(element.c_str()));
    } break;
    case INT32: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int32_t>(element.c_str()));
    } break;
    case INT16: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<int16_t>(element.c_str()));
    } break;
    case FLOAT: {
        value = std::make_unique<Value>(TypeUtils::convertStringToNumber<float_t>(element.c_str()));
    } break;
    case DOUBLE: {
        value =
            std::make_unique<Value>(TypeUtils::convertStringToNumber<double_t>(element.c_str()));
    } break;
    case BOOL: {
        transform(element.begin(), element.end(), element.begin(), ::tolower);
        std::istringstream is(element);
        bool b;
        is >> std::boolalpha >> b;
        value = std::make_unique<Value>(b);
    } break;
    case STRING: {
        value = make_unique<Value>(element);
    } break;
    case DATE: {
        value = std::make_unique<Value>(Date::FromCString(element.c_str(), element.length()));
    } break;
    case TIMESTAMP: {
        value = std::make_unique<Value>(Timestamp::FromCString(element.c_str(), element.length()));
    } break;
    case INTERVAL: {
        value = std::make_unique<Value>(Interval::FromCString(element.c_str(), element.length()));
    } break;
    case VAR_LIST: {
        value = getArrowVarList(element, 1, element.length() - 2, type, copyDescription);
    } break;
    default:
        throw CopyException(
            "Unsupported data type " + Types::dataTypeToString(type.typeID) + " inside LIST");
    }
    return value;
}

} // namespace storage
} // namespace kuzu
