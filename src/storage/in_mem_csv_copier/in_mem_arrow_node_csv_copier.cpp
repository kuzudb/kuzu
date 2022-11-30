#include "include/in_mem_arrow_node_csv_copier.h"

#include "include/copy_csv_task.h"

#include "src/storage/storage_structure/include/in_mem_file.h"

#include <iostream>

namespace kuzu {
namespace storage {

    InMemArrowNodeCSVCopier::InMemArrowNodeCSVCopier(CSVDescription &csvDescription, string outputDirectory,
                                                     TaskScheduler &taskScheduler, Catalog &catalog, table_id_t tableID,
                                                     NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs)
            : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
              numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
        nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
    }

    uint64_t InMemArrowNodeCSVCopier::copy() {
        switch (getFileType(csvDescription.filePath)) {
            case 0: {
                std::cout << "reading CSV file!!!! " << std::endl;
                return copyFromCSVFile();
            }
                break;
            case 1: {
                std::cout << "reading ARROW file!!!! " << std::endl;
                return copyFromArrowFile();
            }
                break;
            case 2: {
                std::cout << "reading PARQUET file!!!!" << std::endl;
                return copyFromParquetFile();
            }
                break;
            default: {
                throw CopyCSVException("Unsupported file type: " + csvDescription.filePath);
            }
        }
    }

    uint64_t InMemArrowNodeCSVCopier::copyFromCSVFile() {
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
        std::cout << "read time:  " << read_time.count() << std::endl;
        std::cout << "write time:  " << write_time.count() << std::endl;
        std::cout << "total time:  " << total_time.count() << std::endl;
        return numNodes;
    }

    uint64_t InMemArrowNodeCSVCopier::copyFromArrowFile() {
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
        std::cout << "read time:  " << read_time.count() << std::endl;
        std::cout << "write time:  " << write_time.count() << std::endl;
        std::cout << "total time:  " << total_time.count() << std::endl;
        return numNodes;
    }

    uint64_t InMemArrowNodeCSVCopier::copyFromParquetFile() {
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
        std::cout << "read time:  " << read_time.count() << std::endl;
        std::cout << "write time:  " << write_time.count() << std::endl;
        std::cout << "total time:  " << total_time.count() << std::endl;

        return numNodes;
    }


    arrow::Status InMemArrowNodeCSVCopier::initArrowCSVReader(
            shared_ptr<arrow::csv::StreamingReader> &csv_streaming_reader,
            const std::string &filePath) {
        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
        auto arrowRead = arrow::csv::ReadOptions::Defaults();
        if (!csvDescription.csvReaderConfig.hasHeader) {
            arrowRead.autogenerate_column_names = true;
        }
        ARROW_ASSIGN_OR_RAISE(
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrowRead,
                        arrow::csv::ParseOptions::Defaults(),
                        arrow::csv::ConvertOptions::Defaults()
                ));
        return arrow::Status::OK();
    }

    arrow::Status InMemArrowNodeCSVCopier::initializeArrowCSV(const std::string &filePath) {
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

    arrow::Status InMemArrowNodeCSVCopier::initializeArrowArrow(std::shared_ptr<arrow::io::ReadableFile> &infile,
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

    void InMemArrowNodeCSVCopier::initializeArrowParquet(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                                         std::unique_ptr<parquet::arrow::FileReader> &reader) {
        PARQUET_ASSIGN_OR_THROW(infile,
                                arrow::io::ReadableFile::Open(csvDescription.filePath,
                                                              arrow::default_memory_pool()));
        PARQUET_THROW_NOT_OK(
                parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

        numBlocks = reader->num_row_groups();
        numLinesPerBlock.resize(numBlocks);
        std::cout << "num row groups: " << numBlocks << std::endl;
        std::shared_ptr<arrow::Table> table;

        for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
            PARQUET_THROW_NOT_OK(reader->RowGroup(blockId)->ReadTable(&table));
            std::cout << "block id: " << blockId << " has num rows: " << table->num_rows() << std::endl;
            numLinesPerBlock[blockId] = table->num_rows();
            numNodes += table->num_rows();
        }
    }

    uint64_t InMemArrowNodeCSVCopier::getFileType(std::string const &fileName) {
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

    void InMemArrowNodeCSVCopier::initializeColumnsAndList() {
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
    arrow::Status InMemArrowNodeCSVCopier::csvPopulateColumns() {
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
    InMemArrowNodeCSVCopier::arrowPopulateColumns(const shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader) {
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
    void InMemArrowNodeCSVCopier::parquetPopulateColumns(const std::unique_ptr<parquet::arrow::FileReader> &reader) {
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
    void InMemArrowNodeCSVCopier::addIDsToIndex(InMemColumn *column, HashIndexBuilder<T> *hashIndex,
                                                node_offset_t startOffset, uint64_t numValues) {
        for (auto i = 0u; i < numValues; i++) {
            auto offset = i + startOffset;
            if constexpr (is_same<T, int64_t>::value) {
                auto key = (int64_t *) column->getElement(offset);
                if (!hashIndex->append(*key, offset)) {
                    throw CopyCSVException("ID value " + to_string(*key) +
                                           " violates the uniqueness constraint for the ID property.");
                }
            } else {
                auto element = (ku_string_t *) column->getElement(offset);
                auto key = column->getInMemOverflowFile()->readString(element);
                if (!hashIndex->append(key.c_str(), offset)) {
                    throw CopyCSVException("ID value  " + key +
                                           " violates the uniqueness constraint for the ID property.");
                }
            }
        }
    }

    template<typename T>
    void InMemArrowNodeCSVCopier::populatePKIndex(InMemColumn *column, HashIndexBuilder<T> *pkIndex,
                                                  node_offset_t startOffset, uint64_t numValues) {
        addIDsToIndex(column, pkIndex, startOffset, numValues);
    }

    template<typename T1, typename T2>
    arrow::Status InMemArrowNodeCSVCopier::batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
                                                                    uint64_t blockId,
                                                                    uint64_t offsetStart,
                                                                    HashIndexBuilder<T1> *pkIndex,
                                                                    InMemArrowNodeCSVCopier *copier,
                                                                    const vector<shared_ptr<T2>> &batchColumns) {
        copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
        // TODO: Consider skip header
//        skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
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
    void InMemArrowNodeCSVCopier::putPropsOfLineIntoColumns(
            vector<unique_ptr<InMemColumn>> &structuredColumns,
            const vector<Property> &structuredProperties, vector<PageByteCursor> &overflowCursors,
            const std::vector<shared_ptr<T>> &arrow_columns,
            uint64_t nodeOffset, uint64_t bufferOffset) {
        for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
            auto column = structuredColumns[columnIdx].get();
            auto currentToken = arrow_columns[columnIdx]->GetScalar(bufferOffset);

            switch (column->getDataType().typeID) {
                case INT64: {
                    if ((*currentToken)->is_valid) {
                        int64_t int64Val = std::dynamic_pointer_cast<arrow::Int64Scalar>(*currentToken)->value;
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&int64Val));
                    }
                }
                    break;
                case DOUBLE: {
                    if ((*currentToken)->is_valid) {
                        double_t val = std::dynamic_pointer_cast<arrow::DoubleScalar>(*currentToken)->value;
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                }
                    break;
                case BOOL: {
                    if ((*currentToken)->is_valid) {
                        bool val = std::dynamic_pointer_cast<arrow::BooleanScalar>(*currentToken)->value;
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&val));
                    }
                }
                    break;
                case DATE: {
                    if ((*currentToken)->is_valid) {
                        string val = currentToken->get()->ToString();
                        date_t dateVal = Date::FromCString(val.c_str(), val.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&dateVal));
                    }
                }
                    break;
                case TIMESTAMP: {
                    if ((*currentToken)->is_valid) {
                        string val = currentToken->get()->ToString();
                        timestamp_t timeVal = Timestamp::FromCString(val.c_str(), val.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&timeVal));
                    }
                }
                    break;
                case INTERVAL: {
                    if ((*currentToken)->is_valid) {
                        string val = currentToken->get()->ToString();
                        interval_t intervalVal = Interval::FromCString(val.c_str(), val.length());
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&intervalVal));
                    }
                }
                    break;
                case STRING: {
                    if ((*currentToken)->is_valid) {
                        string val = currentToken->get()->ToString();
                        auto stringVal = column->getInMemOverflowFile()->copyString(val.c_str(),
                                                                                    overflowCursors[columnIdx]);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&stringVal));
                    }
                }
                    break;
                case LIST: {
                    if ((*currentToken)->is_valid) {
                        auto val = currentToken->get()->ToString();
                        Literal listVal = getArrowList(val, column->getDataType());
                        auto kuList =
                                column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&kuList));
                    }
                }
                    break;
                default:
                    break;
            }
        }
    }


    Literal InMemArrowNodeCSVCopier::getArrowList(string l, const DataType &dataType) {
        //remove "[" and "]"
        Literal result(DataType(LIST, make_unique<DataType>(dataType)));
//        const char delimiter = csvDescription.csvReaderConfig.tokenSeparator;
        //TODO: change delimiter
        const char delimiter = ',';
        vector<string> split;
        int bracket = 0, last = 0;
        if (dataType.typeID == LIST) {
            l = l.substr(1, l.length() - 2);
            for (int i = 0; i < l.length(); i++) {
                if (l[i] == '[') {
                    bracket += 1;
                } else if (l[i] == ']') {
                    bracket -= 1;
                } else if (bracket == 0 && l[i] == delimiter) {
                    split.emplace_back(l.substr(last, i - last));
                    last = i + 1;
                }
            }
        }
        split.emplace_back(l.substr(last, l.length() - last));

        auto childDataType = *dataType.childType;
        for (auto &element: split) {
            switch (childDataType.typeID) {
                case INT64: {
                    result.listVal.emplace_back((int64_t) std::stoll(element));
                }
                    break;
                case DOUBLE: {
                    result.listVal.emplace_back(std::stod(element));
                }
                    break;
                case BOOL: {
                    result.listVal.emplace_back((bool) stoi(element));
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
                    result.listVal.emplace_back(getArrowList(element, *dataType.childType));
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


    void InMemArrowNodeCSVCopier::saveToFile() {
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
