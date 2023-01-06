#include "storage/in_mem_csv_copier/in_mem_arrow_node_copier.h"

#include "storage/in_mem_csv_copier/copy_csv_task.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

InMemArrowNodeCopier::InMemArrowNodeCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
    : InMemStructuresCopier{csvDescription, std::move(outputDirectory), taskScheduler, catalog},
      numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
    nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);

    // TODO: refactor this when implementing reading rel files.
    setFileType(csvDescription.filePath);
}

uint64_t InMemArrowNodeCopier::copy() {
    auto read_start = std::chrono::high_resolution_clock::now();
    logger->info("Reading " + getFileTypeName(inputFileType) + " file.");

    countNumLines(csvDescription.filePath);
    initializeColumnsAndList();

    arrow::Status status;

    switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
    case INT64: {
        status = populateColumns<int64_t>();
    } break;
    case STRING: {
        status = populateColumns<ku_string_t>();
    } break;
    default: {
        throw CopyCSVException("Unsupported data type " +
                               Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                               " for the ID index.");
    }
    }

    if (!status.ok()) {
        throw CopyCSVException(status.ToString());
    }

    auto read_end = std::chrono::high_resolution_clock::now();
    auto write_start = std::chrono::high_resolution_clock::now();

    saveToFile();
    nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
    logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
        nodeTableSchema->tableID);

    auto write_end = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start);
    auto write_time =
        std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start);
    auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - read_start);
    logger->debug("read time: {}.", read_time.count());
    logger->debug("write time: {}.", write_time.count());
    logger->debug("total time: {}.", total_time.count());
    return numNodes;
}

arrow::Status InMemArrowNodeCopier::initCSVReader(
    shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader, const std::string& filePath) {
    shared_ptr<arrow::io::InputStream> arrow_input_stream;
    ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
    auto arrowRead = arrow::csv::ReadOptions::Defaults();

    // TODO: Refactor this once implement reading rel files.
    if (!csvDescription.csvReaderConfig.hasHeader) {
        arrowRead.autogenerate_column_names = true;
    }

    auto arrowConvert = arrow::csv::ConvertOptions::Defaults();
    arrowConvert.strings_can_be_null = true;
    arrowConvert.quoted_strings_can_be_null = false;

    auto arrowParse = arrow::csv::ParseOptions::Defaults();
    arrowParse.delimiter = csvDescription.csvReaderConfig.tokenSeparator;
    arrowParse.escape_char = csvDescription.csvReaderConfig.escapeChar;
    arrowParse.quote_char = csvDescription.csvReaderConfig.quoteChar;
    arrowParse.escaping = true;

    ARROW_ASSIGN_OR_RAISE(
        csv_streaming_reader, arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                  arrow_input_stream, arrowRead, arrowParse, arrowConvert));
    return arrow::Status::OK();
}

arrow::Status InMemArrowNodeCopier::initArrowReader(
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;

    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));
    return arrow::Status::OK();
}

arrow::Status InMemArrowNodeCopier::initParquetReader(
    std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return arrow::Status::OK();
}

void InMemArrowNodeCopier::countNumLines(const std::string& filePath) {
    arrow::Status status;
    switch (inputFileType) {
    case FileTypes::CSV:
        status = countNumLinesCSV(filePath);
        break;
    case FileTypes::ARROW:
        status = countNumLinesArrow(filePath);
        break;
    case FileTypes::PARQUET:
        status = countNumLinesParquet(filePath);
        break;
    }
    if (!status.ok()) {
        throw CopyCSVException(status.ToString());
    }
}

arrow::Status InMemArrowNodeCopier::countNumLinesCSV(const std::string& filePath) {
    shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
    auto status = initCSVReader(csv_streaming_reader, filePath);

    numNodes = 0;
    numBlocks = 0;
    std::shared_ptr<arrow::RecordBatch> currBatch;

    auto endIt = csv_streaming_reader->end();
    for (auto it = csv_streaming_reader->begin(); it != endIt; ++it) {
        ARROW_ASSIGN_OR_RAISE(currBatch, *it);
        ++numBlocks;
        auto currNumRows = currBatch->num_rows();
        numLinesPerBlock.push_back(currNumRows);
        numNodes += currNumRows;
    }
    return status;
}

arrow::Status InMemArrowNodeCopier::countNumLinesArrow(const std::string& filePath) {
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReader(ipc_reader, filePath);

    numNodes = 0;
    numBlocks = ipc_reader->num_record_batches();
    numLinesPerBlock.resize(numBlocks);
    std::shared_ptr<arrow::RecordBatch> rbatch;

    for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
        ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(blockId));
        numLinesPerBlock[blockId] = rbatch->num_rows();
        numNodes += rbatch->num_rows();
    }
    return status;
}

arrow::Status InMemArrowNodeCopier::countNumLinesParquet(const std::string& filePath) {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReader(reader, filePath);

    numNodes = 0;
    numBlocks = reader->num_row_groups();
    numLinesPerBlock.resize(numBlocks);
    std::shared_ptr<arrow::Table> table;

    for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
        ARROW_RETURN_NOT_OK(reader->RowGroup(blockId)->ReadTable(&table));
        numLinesPerBlock[blockId] = table->num_rows();
        numNodes += table->num_rows();
    }
    return status;
}

void InMemArrowNodeCopier::setFileType(std::string const& fileName) {
    auto csvSuffix = getFileTypeSuffix(FileTypes::CSV);
    auto arrowSuffix = getFileTypeSuffix(FileTypes::ARROW);
    auto parquetSuffix = getFileTypeSuffix(FileTypes::PARQUET);

    if (fileName.length() >= csvSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - csvSuffix.length(), csvSuffix.length(), csvSuffix)) {
            inputFileType = FileTypes::CSV;
            return;
        }
    }

    if (fileName.length() >= arrowSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - arrowSuffix.length(), arrowSuffix.length(), arrowSuffix)) {
            inputFileType = FileTypes::ARROW;
            return;
        }
    }

    if (fileName.length() >= parquetSuffix.length()) {
        if (!fileName.compare(fileName.length() - parquetSuffix.length(), parquetSuffix.length(),
                parquetSuffix)) {
            inputFileType = FileTypes::PARQUET;
            return;
        }
    }
    throw CopyCSVException("Unsupported file type: " + fileName);
}

void InMemArrowNodeCopier::initializeColumnsAndList() {
    logger->info("Initializing in memory columns.");
    columns.resize(nodeTableSchema->getNumProperties());
    for (auto& property : nodeTableSchema->properties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
            nodeTableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        columns[property.propertyID] =
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
    }
    logger->info("Done initializing in memory columns.");
}

template<typename T>
arrow::Status InMemArrowNodeCopier::populateColumns() {
    logger->info("Populating properties");
    auto pkIndex =
        make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                             nodeTableSchema->tableID, DBFileType::WAL_VERSION),
            nodeTableSchema->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numNodes);

    arrow::Status status;
    switch (inputFileType) {
    case FileTypes::CSV:
        status = populateColumnsFromCSV<T>(pkIndex);
        break;
    case FileTypes::ARROW:
        status = populateColumnsFromArrow<T>(pkIndex);
        break;
    case FileTypes::PARQUET:
        status = populateColumnsFromParquet<T>(pkIndex);
        break;
    }

    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating properties, constructing the pk index.");
    return status;
}

template<typename T>
arrow::Status InMemArrowNodeCopier::populateColumnsFromCSV(
    unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    node_offset_t offsetStart = 0;

    shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
    auto status = initCSVReader(csv_streaming_reader, csvDescription.filePath);
    if (!status.ok()) {
        return status;
    }

    std::shared_ptr<arrow::RecordBatch> currBatch;

    int blockIdx = 0;
    auto it = csv_streaming_reader->begin();
    auto endIt = csv_streaming_reader->end();
    while (it != endIt) {
        for (int i = 0; i < CopyCSVConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (it == endIt) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(
                CopyCSVTaskFactory::createCopyCSVTask(batchPopulateColumnsTask<T, arrow::Array>,
                    nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, pkIndex.get(),
                    this, currBatch->columns(), csvDescription.csvReaderConfig.tokenSeparator));
            offsetStart += currBatch->num_rows();
            ++blockIdx;
            ++it;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyCSVConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
arrow::Status InMemArrowNodeCopier::populateColumnsFromArrow(
    unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    node_offset_t offsetStart = 0;

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReader(ipc_reader, csvDescription.filePath);
    if (!status.ok()) {
        return status;
    }

    std::shared_ptr<arrow::RecordBatch> currBatch;

    int blockIdx = 0;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyCSVConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, ipc_reader->ReadRecordBatch(blockIdx));
            taskScheduler.scheduleTask(
                CopyCSVTaskFactory::createCopyCSVTask(batchPopulateColumnsTask<T, arrow::Array>,
                    nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, pkIndex.get(),
                    this, currBatch->columns(), csvDescription.csvReaderConfig.tokenSeparator));
            offsetStart += currBatch->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyCSVConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
arrow::Status InMemArrowNodeCopier::populateColumnsFromParquet(
    unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    node_offset_t offsetStart = 0;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReader(reader, csvDescription.filePath);
    if (!status.ok()) {
        return status;
    }

    std::shared_ptr<arrow::Table> currTable;
    int blockIdx = 0;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyCSVConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                batchPopulateColumnsTask<T, arrow::ChunkedArray>,
                nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, pkIndex.get(), this,
                currTable->columns(), csvDescription.csvReaderConfig.tokenSeparator));
            offsetStart += currTable->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyCSVConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
void InMemArrowNodeCopier::populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
    node_offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (is_same<T, int64_t>::value) {
            auto key = (int64_t*)column->getElement(offset);
            if (!pkIndex->append(*key, offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(to_string(*key)));
            }
        } else {
            auto element = (ku_string_t*)column->getElement(offset);
            auto key = column->getInMemOverflowFile()->readString(element);
            if (!pkIndex->append(key.c_str(), offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(key));
            }
        }
    }
}

template<typename T1, typename T2>
arrow::Status InMemArrowNodeCopier::batchPopulateColumnsTask(uint64_t primaryKeyPropertyId,
    uint64_t blockId, uint64_t offsetStart, HashIndexBuilder<T1>* pkIndex,
    InMemArrowNodeCopier* copier, const vector<shared_ptr<T2>>& batchColumns, char delimiter) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumProperties());
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        putPropsOfLineIntoColumns(copier->columns, overflowCursors, batchColumns,
            offsetStart + blockOffset, blockOffset, delimiter);
    }
    populatePKIndex(copier->columns[primaryKeyPropertyId].get(), pkIndex, offsetStart,
        copier->numLinesPerBlock[blockId]);
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    return arrow::Status::OK();
}

template<typename T>
void InMemArrowNodeCopier::putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
    vector<PageByteCursor>& overflowCursors, const std::vector<shared_ptr<T>>& arrow_columns,
    uint64_t nodeOffset, uint64_t blockOffset, char delimiter) {
    for (auto columnIdx = 0u; columnIdx < columns.size(); columnIdx++) {
        auto column = columns[columnIdx].get();
        auto currentToken = arrow_columns[columnIdx]->GetScalar(blockOffset);
        if ((*currentToken)->is_valid) {
            auto stringToken = currentToken->get()->ToString();
            const char* data = stringToken.c_str();

            switch (column->getDataType().typeID) {
            case INT64: {
                int64_t val = TypeUtils::convertToInt64(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case DOUBLE: {
                double_t val = TypeUtils::convertToDouble(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case BOOL: {
                bool val = TypeUtils::convertToBoolean(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case DATE: {
                date_t val = Date::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case TIMESTAMP: {
                timestamp_t val = Timestamp::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case INTERVAL: {
                interval_t val = Interval::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case STRING: {
                // TODO: Refactor this once implement reading rel files.
                stringToken = stringToken.substr(0, DEFAULT_PAGE_SIZE);
                data = stringToken.c_str();

                auto val =
                    column->getInMemOverflowFile()->copyString(data, overflowCursors[columnIdx]);

                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case LIST: {
                Literal listVal = getArrowList(
                    stringToken, 1, stringToken.length() - 2, column->getDataType(), delimiter);
                auto kuList =
                    column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
            } break;
            default:
                break;
            }
        }
    }
}

Literal InMemArrowNodeCopier::getArrowList(
    string& l, int64_t from, int64_t to, const DataType& dataType, char delimiter) {
    auto childDataType = *dataType.childType;
    Literal result(DataType(LIST, make_unique<DataType>(childDataType)));

    vector<pair<int64_t, int64_t>> split;
    int bracket = 0;
    int64_t last = from;
    if (dataType.typeID == LIST) {
        for (int64_t i = from; i <= to; i++) {
            if (l[i] == '[') {
                bracket += 1;
            } else if (l[i] == ']') {
                bracket -= 1;
            } else if (bracket == 0 && l[i] == delimiter) {
                split.emplace_back(last, i - last);
                last = i + 1;
            }
        }
    }
    split.emplace_back(last, to - last + 1);

    for (auto pair : split) {
        string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        switch (childDataType.typeID) {
        case INT64: {
            result.listVal.emplace_back((int64_t)stoll(element));
        } break;
        case DOUBLE: {
            result.listVal.emplace_back(stod(element));
        } break;
        case BOOL: {
            transform(element.begin(), element.end(), element.begin(), ::tolower);
            std::istringstream is(element);
            bool b;
            is >> std::boolalpha >> b;
            result.listVal.emplace_back(b);
        } break;
        case STRING: {
            result.listVal.emplace_back(element);
        } break;
        case DATE: {
            result.listVal.emplace_back(Date::FromCString(element.c_str(), element.length()));
        } break;
        case TIMESTAMP: {
            result.listVal.emplace_back(Timestamp::FromCString(element.c_str(), element.length()));
        } break;
        case INTERVAL: {
            result.listVal.emplace_back(Interval::FromCString(element.c_str(), element.length()));
        } break;
        case LIST: {
            result.listVal.emplace_back(getArrowList(
                l, pair.first + 1, pair.second + pair.first - 1, *dataType.childType, delimiter));
        } break;
        default:
            throw CSVReaderException("Unsupported data type " +
                                     Types::dataTypeToString(dataType.childType->typeID) +
                                     " inside LIST");
        }
    }
    auto numBytesOfOverflow = result.listVal.size() * Types::getDataTypeSize(dataType.typeID);
    if (numBytesOfOverflow >= DEFAULT_PAGE_SIZE) {
        throw CSVReaderException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            numBytesOfOverflow));
    }
    return result;
}

void InMemArrowNodeCopier::saveToFile() {
    logger->debug("Writing node columns to disk.");
    assert(!columns.empty());
    for (auto& column : columns) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            [&](InMemColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node columns to disk.");
}

std::string InMemArrowNodeCopier::getFileTypeName(FileTypes fileTypes) {
    switch (fileTypes) {
    case FileTypes::CSV:
        return "csv";
    case FileTypes::ARROW:
        return "arrow";
    case FileTypes::PARQUET:
        return "parquet";
    }
}

std::string InMemArrowNodeCopier::getFileTypeSuffix(FileTypes fileTypes) {
    return "." + getFileTypeName(fileTypes);
}

} // namespace storage
} // namespace kuzu
