#include "storage/copier/table_copier.h"

#include "common/constants.h"
#include "common/string_utils.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

TableCopier::TableCopier(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, common::table_id_t tableID,
    TablesStatistics* tablesStatistics)
    : logger{LoggerUtils::getLogger(LoggerConstants::LoggerEnum::LOADER)},
      copyDescription{copyDescription}, outputDirectory{std::move(outputDirectory)},
      taskScheduler{taskScheduler}, catalog{catalog}, numRows{0},
      tableSchema{catalog.getReadOnlyVersion()->getTableSchema(tableID)}, tablesStatistics{
                                                                              tablesStatistics} {}

uint64_t TableCopier::copy(processor::ExecutionContext* executionContext) {
    logger->info(StringUtils::string_format("Copying {} file to table {}.",
        CopyDescription::getFileTypeName(copyDescription.fileType), tableSchema->tableName));
    populateInMemoryStructures(executionContext);
    updateTableStatistics();
    saveToFile();
    logger->info("Done copying file to table {}.", tableSchema->tableName);
    return numRows;
}

void TableCopier::populateInMemoryStructures(processor::ExecutionContext* executionContext) {
    countNumLines(copyDescription.filePaths);
    initializeColumnsAndLists();
    populateColumnsAndLists(executionContext);
}

void TableCopier::countNumLines(const std::vector<std::string>& filePaths) {
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        countNumLinesCSV(filePaths);
    } break;
    case CopyDescription::FileType::PARQUET: {
        countNumLinesParquet(filePaths);
    } break;
    default: {
        throw CopyException{StringUtils::string_format("Unrecognized file type: {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType))};
    }
    }
}

void TableCopier::countNumLinesCSV(const std::vector<std::string>& filePaths) {
    numRows = 0;
    for (auto& filePath : filePaths) {
        auto csvStreamingReader = initCSVReader(filePath);
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

void TableCopier::countNumLinesParquet(const std::vector<std::string>& filePaths) {
    numRows = 0;
    for (auto& filePath : filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader = initParquetReader(filePath);
        uint64_t numBlocks = reader->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock;
        std::shared_ptr<arrow::Table> table;
        auto startNodeOffset = numRows;
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            throwCopyExceptionIfNotOK(reader->RowGroup(blockIdx)->ReadTable(&table));
            numLinesPerBlock.push_back(table->num_rows());
            numRows += table->num_rows();
        }
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
    }
}

std::shared_ptr<arrow::csv::StreamingReader> TableCopier::initCSVReader(
    const std::string& filePath) const {
    std::shared_ptr<arrow::io::InputStream> inputStream;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&inputStream));
    auto csvReadOptions = arrow::csv::ReadOptions::Defaults();
    csvReadOptions.block_size = CopyConstants::CSV_READING_BLOCK_SIZE;
    if (!tableSchema->isNodeTable) {
        csvReadOptions.column_names.emplace_back("_FROM");
        csvReadOptions.column_names.emplace_back("_TO");
    }
    for (auto& property : tableSchema->properties) {
        if (!TableSchema::isReservedPropertyName(property.name)) {
            csvReadOptions.column_names.push_back(property.name);
        }
    }
    if (copyDescription.csvReaderConfig->hasHeader) {
        csvReadOptions.skip_rows = 1;
    }

    auto csvParseOptions = arrow::csv::ParseOptions::Defaults();
    csvParseOptions.delimiter = copyDescription.csvReaderConfig->delimiter;
    csvParseOptions.escape_char = copyDescription.csvReaderConfig->escapeChar;
    csvParseOptions.quote_char = copyDescription.csvReaderConfig->quoteChar;
    csvParseOptions.ignore_empty_lines = false;
    csvParseOptions.escaping = true;

    auto csvConvertOptions = arrow::csv::ConvertOptions::Defaults();
    csvConvertOptions.strings_can_be_null = true;
    // Only the empty string is treated as NULL.
    csvConvertOptions.null_values = {""};
    csvConvertOptions.quoted_strings_can_be_null = false;
    for (auto& property : tableSchema->properties) {
        if (property.name == "_FROM" || property.name == "_TO") {
            csvConvertOptions.column_types[property.name] = arrow::int64();
            continue;
        }
        if (!TableSchema::isReservedPropertyName(property.name)) {
            csvConvertOptions.column_types[property.name] = toArrowDataType(property.dataType);
        }
    }

    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
    throwCopyExceptionIfNotOK(arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
        inputStream, csvReadOptions, csvParseOptions, csvConvertOptions)
                                  .Value(&csvStreamingReader));
    return csvStreamingReader;
}

std::unique_ptr<parquet::arrow::FileReader> TableCopier::initParquetReader(
    const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    throwCopyExceptionIfNotOK(arrow::io::ReadableFile::Open(filePath).Value(&infile));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    throwCopyExceptionIfNotOK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return reader;
}

std::vector<std::pair<int64_t, int64_t>> TableCopier::getListElementPos(
    const std::string& l, int64_t from, int64_t to, CopyDescription& copyDescription) {
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

std::unique_ptr<Value> TableCopier::getArrowVarList(const std::string& l, int64_t from, int64_t to,
    const DataType& dataType, CopyDescription& copyDescription) {
    assert(dataType.typeID == common::VAR_LIST || dataType.typeID == common::FIXED_LIST);
    auto split = getListElementPos(l, from, to, copyDescription);
    std::vector<std::unique_ptr<Value>> values;
    auto childDataType = *dataType.getChildType();
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        std::unique_ptr<Value> value;
        switch (childDataType.typeID) {
        case INT64: {
            value = std::make_unique<Value>((int64_t)stoll(element));
        } break;
        case DOUBLE: {
            value = std::make_unique<Value>(stod(element));
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
            value =
                std::make_unique<Value>(Timestamp::FromCString(element.c_str(), element.length()));
        } break;
        case INTERVAL: {
            value =
                std::make_unique<Value>(Interval::FromCString(element.c_str(), element.length()));
        } break;
        case VAR_LIST: {
            value = getArrowVarList(l, pair.first + 1, pair.second + pair.first - 1,
                *dataType.getChildType(), copyDescription);
        } break;
        default:
            throw ReaderException("Unsupported data type " +
                                  Types::dataTypeToString(dataType.getChildType()->typeID) +
                                  " inside LIST");
        }
        values.push_back(std::move(value));
    }
    auto numBytesOfOverflow = values.size() * Types::getDataTypeSize(childDataType.typeID);
    if (numBytesOfOverflow >= BufferPoolConstants::PAGE_4KB_SIZE) {
        throw ReaderException(StringUtils::string_format(
            "Maximum num bytes of a LIST is {}. Input list's num bytes is {}.",
            BufferPoolConstants::PAGE_4KB_SIZE, numBytesOfOverflow));
    }
    return make_unique<Value>(
        DataType(VAR_LIST, std::make_unique<DataType>(childDataType)), std::move(values));
}

std::unique_ptr<uint8_t[]> TableCopier::getArrowFixedList(const std::string& l, int64_t from,
    int64_t to, const DataType& dataType, CopyDescription& copyDescription) {
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
            throw ReaderException("Unsupported data type " +
                                  Types::dataTypeToString(dataType.getChildType()->typeID) +
                                  " inside FIXED_LIST");
        }
        }
    }
    auto extraTypeInfo = reinterpret_cast<FixedListTypeInfo*>(dataType.getExtraTypeInfo());
    if (numElementsRead != extraTypeInfo->getFixedNumElementsInList()) {
        throw ReaderException(StringUtils::string_format(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            extraTypeInfo->getFixedNumElementsInList(), numElementsRead));
    }
    return listVal;
}

void TableCopier::throwCopyExceptionIfNotOK(const arrow::Status& status) {
    if (!status.ok()) {
        throw CopyException(status.ToString());
    }
}

std::shared_ptr<arrow::DataType> TableCopier::toArrowDataType(const common::DataType& dataType) {
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
    case common::FIXED_LIST:
    case common::VAR_LIST:
    case common::STRING:
    case common::INTERVAL: {
        return arrow::utf8();
    }
    default: {
        throw CopyException(
            "Unsupported data type for CSV " + Types::dataTypeToString(dataType.typeID));
    }
    }
}

} // namespace storage
} // namespace kuzu
