#include "storage/in_mem_csv_copier/in_mem_arrow_node_copier.h"

#include "storage/in_mem_csv_copier/copy_csv_task.h"

#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {
    //TODO: remove this once implement reading rel files
    //set as consts as for now. Will change to better implementation once implement reading rel files.
    char delimiter;
    int default_page_size = DEFAULT_PAGE_SIZE;

    InMemArrowNodeCopier::InMemArrowNodeCopier(CSVDescription &csvDescription, string outputDirectory,
                                               TaskScheduler &taskScheduler, Catalog &catalog, table_id_t tableID,
                                               NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs)
            : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
              numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
        nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);

        //TODO: remove this once implement reading rel files
        delimiter = csvDescription.csvReaderConfig.tokenSeparator;
    }

    uint64_t InMemArrowNodeCopier::copy() {
        switch (getFileType(csvDescription.filePath)) {
            case 0: {
                logger->info("Reading CSV file.");
                return copyFromCSVFile();
            }
                break;
            case 1: {
                logger->info("Reading ARROW file.");
                return copyFromArrowFile();
            }
                break;
            case 2: {
                logger->info("Reading PARQUET file.");
                return copyFromParquetFile();
            }
                break;
            default: {
                throw CopyCSVException("Unsupported file type: " + csvDescription.filePath);
            }
        }
    }

    uint64_t InMemArrowNodeCopier::copyFromCSVFile() {
        auto read_start = std::chrono::high_resolution_clock::now();
        arrow::Status status;
        status = initializeArrowCSV(csvDescription.filePath);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        initializeColumnsAndList();

        switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
            case INT64: {
                status = csvPopulateColumns<int64_t>();
            }
                break;
            case STRING: {
                status = csvPopulateColumns<ku_string_t>();
            }
                break;
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
        auto write_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start);
        auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - read_start);
        logger->debug("read time: {}.", read_time.count());
        logger->debug("write time: {}.", write_time.count());
        logger->debug("total time: {}.", total_time.count());
        return numNodes;
    }

    uint64_t InMemArrowNodeCopier::copyFromArrowFile() {
        auto read_start = std::chrono::high_resolution_clock::now();
        std::shared_ptr<arrow::io::ReadableFile> infile;
        shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
        auto status = initializeArrowArrow(infile, ipc_reader);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        initializeColumnsAndList();

        switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
            case INT64: {
                status = arrowPopulateColumns<int64_t>(ipc_reader);
            }
                break;
            case STRING: {
                status = arrowPopulateColumns<ku_string_t>(ipc_reader);
            }
                break;
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
        auto write_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start);
        auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - read_start);
        logger->debug("read time: {}.", read_time.count());
        logger->debug("write time: {}.", write_time.count());
        logger->debug("total time: {}.", total_time.count());
        return numNodes;
    }

    uint64_t InMemArrowNodeCopier::copyFromParquetFile() {
        auto read_start = std::chrono::high_resolution_clock::now();
        std::shared_ptr<arrow::io::ReadableFile> infile;
        std::unique_ptr<parquet::arrow::FileReader> reader;
        initializeArrowParquet(infile, reader);

        initializeColumnsAndList();

        switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
            case INT64: {
                parquetPopulateColumns<int64_t>(reader);
            }
                break;
            case STRING: {
                parquetPopulateColumns<ku_string_t>(reader);
            }
                break;
            default: {
                throw CopyCSVException("Unsupported data type " +
                                       Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                                       " for the ID index.");
            }
        }
        auto read_end = std::chrono::high_resolution_clock::now();
        auto write_start = std::chrono::high_resolution_clock::now();
        saveToFile();

        nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
        logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
                     nodeTableSchema->tableID);
        auto write_end = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start);
        auto write_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start);
        auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - read_start);
        logger->debug("read time: {}.", read_time.count());
        logger->debug("write time: {}.", write_time.count());
        logger->debug("total time: {}.", total_time.count());

        return numNodes;
    }


    arrow::Status InMemArrowNodeCopier::initArrowCSVReader(
            shared_ptr<arrow::csv::StreamingReader> &csv_streaming_reader,
            const std::string &filePath) {
        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
        auto arrowRead = arrow::csv::ReadOptions::Defaults();

        //TODO: remove this once implement reading rel files
        arrowRead.block_size = 80000;
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
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrowRead,
                        arrowParse,
                        arrowConvert));
        return arrow::Status::OK();
    }

    arrow::Status InMemArrowNodeCopier::initializeArrowCSV(const std::string &filePath) {
        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        auto status = initArrowCSVReader(csv_streaming_reader, filePath);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        numBlocks = 0;
        numNodes = 0;
        std::shared_ptr<arrow::RecordBatch> currBatch;

        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            ++numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numNodes += currNumRows;
        }

        return arrow::Status::OK();
    }

    arrow::Status InMemArrowNodeCopier::initializeArrowArrow(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                                             std::shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader) {
        ARROW_ASSIGN_OR_RAISE(
                infile,
                arrow::io::ReadableFile::Open(
                        csvDescription.filePath,
                        arrow::default_memory_pool()));

        ARROW_ASSIGN_OR_RAISE(
                ipc_reader,
                arrow::ipc::RecordBatchFileReader::Open(infile));

        numBlocks = ipc_reader->num_record_batches();
        numLinesPerBlock.resize(numBlocks);
        std::shared_ptr<arrow::RecordBatch> rbatch;
        numNodes = 0;
        for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
            ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(blockId));
            numLinesPerBlock[blockId] = rbatch->num_rows();
            numNodes += rbatch->num_rows();
        }

        return arrow::Status::OK();
    }

    void InMemArrowNodeCopier::initializeArrowParquet(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                                      std::unique_ptr<parquet::arrow::FileReader> &reader) {
        PARQUET_ASSIGN_OR_THROW(infile,
                                arrow::io::ReadableFile::Open(csvDescription.filePath,
                                                              arrow::default_memory_pool()));
        PARQUET_THROW_NOT_OK(
                parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

        numBlocks = reader->num_row_groups();
        numLinesPerBlock.resize(numBlocks);
        logger->debug("num row groups: {}.", numBlocks);
        std::shared_ptr<arrow::Table> table;

        for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
            PARQUET_THROW_NOT_OK(reader->RowGroup(blockId)->ReadTable(&table));
            numLinesPerBlock[blockId] = table->num_rows();
            numNodes += table->num_rows();
        }
    }

    uint64_t InMemArrowNodeCopier::getFileType(std::string const &fileName) {
        // type 0: csv file
        // type 1: arrow file
        // type 2: parquet file
        std::string CSV_SUFFIX = ".csv";
        std::string ARROW_SUFFIX = ".arrow";
        std::string PARQUET_SUFFIX = ".parquet";

        if (fileName.length() >= CSV_SUFFIX.length()) {
            if (!fileName.compare(
                    fileName.length() - CSV_SUFFIX.length(),
                    CSV_SUFFIX.length(),
                    CSV_SUFFIX)) {
                return 0;
            }
        }

        if (fileName.length() >= ARROW_SUFFIX.length()) {
            if (!fileName.compare(
                    fileName.length() - ARROW_SUFFIX.length(),
                    ARROW_SUFFIX.length(),
                    ARROW_SUFFIX)) {
                return 1;
            }
        }

        if (fileName.length() >= PARQUET_SUFFIX.length()) {
            if (!fileName.compare(
                    fileName.length() - PARQUET_SUFFIX.length(),
                    PARQUET_SUFFIX.length(),
                    PARQUET_SUFFIX)) {
                return 2;
            }
        }


        throw CopyCSVException("Unsupported file type: " + fileName);
    }

    void InMemArrowNodeCopier::initializeColumnsAndList() {
        logger->info("Initializing in memory structured columns.");
        structuredColumns.resize(nodeTableSchema->getNumStructuredProperties());
        for (auto &property: nodeTableSchema->structuredProperties) {
            auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
                                                                  nodeTableSchema->tableID, property.propertyID,
                                                                  DBFileType::WAL_VERSION);
            structuredColumns[property.propertyID] =
                    InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
        }
        logger->info("Done initializing in memory structured columns.");
    }

    template<typename T>
    arrow::Status InMemArrowNodeCopier::csvPopulateColumns() {
        logger->info("Populating structured properties");
        auto pkIndex =
                make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                                                 nodeTableSchema->tableID,
                                                                                 DBFileType::WAL_VERSION),
                                                 nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numNodes);
        node_offset_t offsetStart = 0;

        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        auto status = initArrowCSVReader(csv_streaming_reader, csvDescription.filePath);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        std::shared_ptr<arrow::RecordBatch> currBatch;


        int blockIdx = 0;
        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    batchPopulateColumnsTask<T, arrow::Array>,
                    nodeTableSchema->primaryKeyPropertyIdx,
                    blockIdx, offsetStart, pkIndex.get(), this, currBatch->columns()));
            offsetStart += currBatch->num_rows();
            ++blockIdx;
        }

        taskScheduler.waitAllTasksToCompleteOrError();
        logger->info("Flush the pk index to disk.");
        pkIndex->flush();
        logger->info("Done populating structured properties, constructing the pk index.");

        return arrow::Status::OK();
    }

    template<typename T>
    arrow::Status
    InMemArrowNodeCopier::arrowPopulateColumns(const shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader) {
        logger->info("Populating structured properties");
        auto pkIndex =
                make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                                                 nodeTableSchema->tableID,
                                                                                 DBFileType::WAL_VERSION),
                                                 nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numNodes);
        node_offset_t offsetStart = 0;

        std::shared_ptr<arrow::RecordBatch> currBatch;

        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            ARROW_ASSIGN_OR_RAISE(currBatch, ipc_reader->ReadRecordBatch(blockIdx));
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    batchPopulateColumnsTask<T, arrow::Array>,
                    nodeTableSchema->primaryKeyPropertyIdx,
                    blockIdx, offsetStart, pkIndex.get(), this, currBatch->columns()));
            offsetStart += currBatch->num_rows();
        }

        taskScheduler.waitAllTasksToCompleteOrError();
        logger->info("Flush the pk index to disk.");
        pkIndex->flush();
        logger->info("Done populating structured properties, constructing the pk index.");

        return arrow::Status::OK();
    }

    template<typename T>
    void InMemArrowNodeCopier::parquetPopulateColumns(const std::unique_ptr<parquet::arrow::FileReader> &reader) {
        logger->info("Populating structured properties");
        auto pkIndex =
                make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                                                 nodeTableSchema->tableID,
                                                                                 DBFileType::WAL_VERSION),
                                                 nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numNodes);
        node_offset_t offsetStart = 0;

        std::shared_ptr<arrow::Table> currTable;
        for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
            PARQUET_THROW_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    batchPopulateColumnsTask<T, arrow::ChunkedArray>,
                    nodeTableSchema->primaryKeyPropertyIdx,
                    blockIdx, offsetStart, pkIndex.get(), this, currTable->columns()));
            offsetStart += currTable->num_rows();
        }

        taskScheduler.waitAllTasksToCompleteOrError();
        logger->info("Flush the pk index to disk.");
        pkIndex->flush();
        logger->info("Done populating structured properties, constructing the pk index.");
    }

    template<typename T>
    void InMemArrowNodeCopier::addIDsToIndex(InMemColumn *column, HashIndexBuilder<T> *hashIndex,
                                             node_offset_t startOffset, uint64_t numValues) {
        for (auto i = 0u; i < numValues; i++) {
            auto offset = i + startOffset;
            if constexpr (is_same<T, int64_t>::value) {
                auto key = (int64_t *) column->getElement(offset);
                if (!hashIndex->append(*key, offset)) {
                    throw CopyCSVException(Exception::getExistedPKExceptionMsg(to_string(*key)));
                }
            } else {
                auto element = (ku_string_t *) column->getElement(offset);
                auto key = column->getInMemOverflowFile()->readString(element);
                if (!hashIndex->append(key.c_str(), offset)) {
                    throw CopyCSVException(Exception::getExistedPKExceptionMsg(key));
                }
            }
        }
    }

    template<typename T>
    void InMemArrowNodeCopier::populatePKIndex(InMemColumn *column, HashIndexBuilder<T> *pkIndex,
                                               node_offset_t startOffset, uint64_t numValues) {
        addIDsToIndex(column, pkIndex, startOffset, numValues);
    }

    template<typename T1, typename T2>
    arrow::Status InMemArrowNodeCopier::batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
                                                                 uint64_t blockId,
                                                                 uint64_t offsetStart,
                                                                 HashIndexBuilder<T1> *pkIndex,
                                                                 InMemArrowNodeCopier *copier,
                                                                 const vector<shared_ptr<T2>> &batchColumns) {
        copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
        for (auto bufferOffset = 0u; bufferOffset < copier->numLinesPerBlock[blockId]; ++bufferOffset) {
            putPropsOfLineIntoColumns(copier->structuredColumns,
                                      copier->nodeTableSchema->structuredProperties,
                                      overflowCursors,
                                      batchColumns,
                                      offsetStart + bufferOffset,
                                      bufferOffset);
        }
        populatePKIndex(copier->structuredColumns[primaryKeyPropertyIdx].get(), pkIndex, offsetStart,
                        copier->numLinesPerBlock[blockId]);
        copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        return arrow::Status::OK();
    }

    template<typename T>
    void InMemArrowNodeCopier::putPropsOfLineIntoColumns(
            vector<unique_ptr<InMemColumn>> &structuredColumns,
            const vector<Property> &structuredProperties, vector<PageByteCursor> &overflowCursors,
            const std::vector<shared_ptr<T>> &arrow_columns,
            uint64_t nodeOffset, uint64_t bufferOffset) {
        for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
            auto column = structuredColumns[columnIdx].get();
            auto currentToken = arrow_columns[columnIdx]->GetScalar(bufferOffset);
            if ((*currentToken)->is_valid) {
                auto stringToken = currentToken->get()->ToString();
                const char* data = stringToken.c_str();

                switch (column->getDataType().typeID) {
                    case INT64: {
                        int64_t val = TypeUtils::convertToInt64(data);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case DOUBLE: {
                        double_t val = TypeUtils::convertToDouble(data);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case BOOL: {
                        bool val = TypeUtils::convertToBoolean(data);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case DATE: {
                        date_t val = Date::FromCString(data, stringToken.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case TIMESTAMP: {
                        timestamp_t val = Timestamp::FromCString(data, stringToken.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case INTERVAL: {
                        interval_t val = Interval::FromCString(data, stringToken.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case STRING: {
                        //TODO: remove this once implement reading rel files
                        stringToken = stringToken.substr(0, default_page_size);
                        data = stringToken.c_str();

                        auto val = column->getInMemOverflowFile()->copyString(data,
                                                                              overflowCursors[columnIdx]);

                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                        break;
                    case LIST: {
                        Literal listVal = getArrowList(stringToken, 1, stringToken.length() - 2, column->getDataType());
                        auto kuList =
                                column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&kuList));
                    }
                        break;
                    default:
                        break;
                }

            }
        }
    }

    Literal InMemArrowNodeCopier::getArrowList(string& l, int64_t from, int64_t to, const DataType &dataType) {
        auto childDataType = *dataType.childType;
        Literal result(DataType(LIST, make_unique<DataType>(childDataType)));

        vector<pair<int64_t, int64_t>> split;
        int bracket = 0;
        int64_t  last = from;
        if (dataType.typeID == LIST) {
            for (int64_t i = from; i <= to; i++) {
                if (l[i] == '[') {
                    bracket += 1;
                } else if (l[i] == ']') {
                    bracket -= 1;
                } else if (bracket == 0 && l[i] == delimiter) {
                    split.emplace_back(make_pair(last, i - last));
                    last = i + 1;
                }
            }
        }
        split.emplace_back(make_pair(last, to - last + 1));

        for (auto pair: split) {
            string element = l.substr(pair.first, pair.second);
            if (element.empty()) {
                continue;
            }
            switch (childDataType.typeID) {
                case INT64: {
                    result.listVal.emplace_back((int64_t) stoll(element));
                }
                    break;
                case DOUBLE: {
                    result.listVal.emplace_back( stod(element));
                }
                    break;
                case BOOL: {
                    transform(element.begin(), element.end(), element.begin(), ::tolower);
                    std::istringstream is(element);
                    bool b;
                    is >> std::boolalpha >> b;
                    result.listVal.emplace_back(b);
                }
                    break;
                case STRING: {
                    result.listVal.emplace_back(element);
                }
                    break;
                case DATE: {
                    result.listVal.emplace_back(Date::FromCString(element.c_str(), element.length()));
                }
                    break;
                case TIMESTAMP: {
                    result.listVal.emplace_back(Timestamp::FromCString(element.c_str(), element.length()));
                }
                    break;
                case INTERVAL: {
                    result.listVal.emplace_back(Interval::FromCString(element.c_str(), element.length()));
                }
                    break;
                case LIST: {
                    result.listVal.emplace_back(getArrowList(l, pair.first + 1,
                                                             pair.second + pair.first - 1, *dataType.childType));
                }
                    break;
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
        logger->debug("Writing node structured columns to disk.");
        assert(!structuredColumns.empty());
        for (auto &column: structuredColumns) {
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    [&](InMemColumn *x) { x->saveToFile(); }, column.get()));
        }
        taskScheduler.waitAllTasksToCompleteOrError();
        logger->debug("Done writing node structured columns to disk.");
    }

} // namespace storage
} // namespace kuzu
