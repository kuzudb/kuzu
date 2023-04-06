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

uint64_t TableCopier::copy() {
    logger->info(StringUtils::string_format("Copying {} file to table {}.",
        CopyDescription::getFileTypeName(copyDescription.fileType), tableSchema->tableName));
    populateInMemoryStructures();
    updateTableStatistics();
    saveToFile();
    logger->info("Done copying file to table {}.", tableSchema->tableName);
    return numRows;
}

void TableCopier::populateInMemoryStructures() {
    countNumLines(copyDescription.filePaths);
    initializeColumnsAndLists();
    populateColumnsAndLists();
}

void TableCopier::countNumLines(const std::vector<std::string>& filePaths) {
    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        status = countNumLinesCSV(filePaths);
    } break;
    case CopyDescription::FileType::PARQUET: {
        status = countNumLinesParquet(filePaths);
    } break;
    default: {
        throw CopyException{StringUtils::string_format("Unrecognized file type: {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType))};
    }
    }
    throwCopyExceptionIfNotOK(status);
}

arrow::Status TableCopier::countNumLinesCSV(const std::vector<std::string>& filePaths) {
    numRows = 0;
    arrow::Status status;
    for (auto& filePath : filePaths) {
        std::shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        status = initCSVReaderAndCheckStatus(csv_streaming_reader, filePath);
        throwCopyExceptionIfNotOK(status);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        uint64_t numBlocks = 0;
        std::vector<uint64_t> numLinesPerBlock;
        auto endIt = csv_streaming_reader->end();
        auto startNodeOffset = numRows;
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numRows += currNumRows;
        }
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
    }
    return status;
}

arrow::Status TableCopier::countNumLinesParquet(const std::vector<std::string>& filePaths) {
    numRows = 0;
    arrow::Status status;
    for (auto& filePath : filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        status = initParquetReaderAndCheckStatus(reader, filePath);
        throwCopyExceptionIfNotOK(status);
        uint64_t numBlocks = reader->num_row_groups();
        std::vector<uint64_t> numLinesPerBlock;
        std::shared_ptr<arrow::Table> table;
        auto startNodeOffset = numRows;
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&table));
            numLinesPerBlock.push_back(table->num_rows());
            numRows += table->num_rows();
        }
        fileBlockInfos.emplace(
            filePath, FileBlockInfo{startNodeOffset, numBlocks, numLinesPerBlock});
    }
    return status;
}

arrow::Status TableCopier::initCSVReaderAndCheckStatus(
    std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
    const std::string& filePath) {
    auto status = initCSVReader(csv_streaming_reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status TableCopier::initCSVReader(
    std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
    const std::string& filePath) {
    std::shared_ptr<arrow::io::InputStream> arrow_input_stream;
    ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
    auto arrowRead = arrow::csv::ReadOptions::Defaults();
    arrowRead.block_size = CopyConstants::CSV_READING_BLOCK_SIZE;
    if (!copyDescription.csvReaderConfig->hasHeader) {
        arrowRead.autogenerate_column_names = true;
    }

    auto arrowConvert = arrow::csv::ConvertOptions::Defaults();
    arrowConvert.strings_can_be_null = true;
    // Only the empty string is treated as NULL.
    arrowConvert.null_values = {""};
    arrowConvert.quoted_strings_can_be_null = false;

    auto arrowParse = arrow::csv::ParseOptions::Defaults();
    arrowParse.delimiter = copyDescription.csvReaderConfig->delimiter;
    arrowParse.escape_char = copyDescription.csvReaderConfig->escapeChar;
    arrowParse.quote_char = copyDescription.csvReaderConfig->quoteChar;
    arrowParse.ignore_empty_lines = false;
    arrowParse.escaping = true;

    ARROW_ASSIGN_OR_RAISE(
        csv_streaming_reader, arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                  arrow_input_stream, arrowRead, arrowParse, arrowConvert));
    return arrow::Status::OK();
}

arrow::Status TableCopier::initArrowReaderAndCheckStatus(
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader, const std::string& filePath) {
    auto status = initArrowReader(ipc_reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status TableCopier::initArrowReader(
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;

    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));
    return arrow::Status::OK();
}

arrow::Status TableCopier::initParquetReaderAndCheckStatus(
    std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath) {
    auto status = initParquetReader(reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status TableCopier::initParquetReader(
    std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));

    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return arrow::Status::OK();
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
    auto childDataType = *dataType.childType;
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
                *dataType.childType, copyDescription);
        } break;
        default:
            throw ReaderException("Unsupported data type " +
                                  Types::dataTypeToString(dataType.childType->typeID) +
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
    auto childDataType = *dataType.childType;
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
                                  Types::dataTypeToString(dataType.childType->typeID) +
                                  " inside FIXED_LIST");
        }
        }
    }
    if (numElementsRead != dataType.fixedNumElementsInList) {
        throw ReaderException(StringUtils::string_format(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            dataType.fixedNumElementsInList, numElementsRead));
    }
    return listVal;
}

void TableCopier::throwCopyExceptionIfNotOK(const arrow::Status& status) {
    if (!status.ok()) {
        throw CopyException(status.ToString());
    }
}

} // namespace storage
} // namespace kuzu
