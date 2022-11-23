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
        arrow::Status status;
        status = initializeArrowCSV(csvDescription.filePath);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        std::vector<string> unstructuredPropertyNames;
        catalog.setUnstructuredPropertiesOfNodeTableSchema(
                unstructuredPropertyNames, nodeTableSchema->tableID);

        initializeColumnsAndList();

        // Populate structured columns with the ID hash index and count the size of unstructured
        // lists.
        switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
            case INT64: {
                status = arrowPopulateColumnsAndCountUnstrPropertyListSizes<int64_t>();
            }
                break;
            case STRING: {
                status = arrowPopulateColumnsAndCountUnstrPropertyListSizes<ku_string_t>();
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

        calcUnstrListsHeadersAndMetadata();
        populateUnstrPropertyLists();

        saveToFile();
        nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
        logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
                     nodeTableSchema->tableID);
        return numNodes;
    }

    arrow::Status InMemArrowNodeCSVCopier::initializeArrowCSV(const std::string &filePath) {
        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
        ARROW_ASSIGN_OR_RAISE(
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrow::csv::ReadOptions::Defaults(),
                        arrow::csv::ParseOptions::Defaults(),
                        arrow::csv::ConvertOptions::Defaults()
                        ));


        numBlocks = 0;
        numNodes = 0;
        std::shared_ptr<arrow::RecordBatch> currBatch;

        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++ it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            ++ numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numNodes += currNumRows;
        }

        return arrow::Status::OK();
    }

    void InMemArrowNodeCSVCopier::initializeColumnsAndList() {
        logger->info("Initializing in memory structured columns and unstructured list.");
        structuredColumns.resize(nodeTableSchema->getNumStructuredProperties());
        for (auto &property: nodeTableSchema->structuredProperties) {
            auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
                                                                  nodeTableSchema->tableID, property.propertyID,
                                                                  DBFileType::WAL_VERSION);
            structuredColumns[property.propertyID] =
                    InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
        }
        unstrPropertyLists = make_unique<InMemUnstructuredLists>(
                StorageUtils::getNodeUnstrPropertyListsFName(
                        outputDirectory, nodeTableSchema->tableID, DBFileType::WAL_VERSION),
                numNodes);
        logger->info("Done initializing in memory structured columns and unstructured list.");
    }

    static vector<string> mergeUnstrPropertyNamesFromBlocks(
            vector<unordered_set<string>> &unstructuredPropertyNamesPerBlock) {
        unordered_set<string> unstructuredPropertyNames;
        for (auto &unstructuredPropertiesInBlock: unstructuredPropertyNamesPerBlock) {
            for (auto &propertyName: unstructuredPropertiesInBlock) {
                unstructuredPropertyNames.insert(propertyName);
            }
        }
        // Ensure the same order in different platforms.
        vector<string> result{unstructuredPropertyNames.begin(), unstructuredPropertyNames.end()};
        sort(result.begin(), result.end());
        return result;
    }

    template<typename T>
    arrow::Status InMemArrowNodeCSVCopier::arrowPopulateColumnsAndCountUnstrPropertyListSizes() {
        logger->info("Populating structured properties and Counting unstructured properties.");
        auto pkIndex =
                make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                                                 nodeTableSchema->tableID,
                                                                                 DBFileType::WAL_VERSION),
                                                 nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numNodes);
        node_offset_t offsetStart = 0;

        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(csvDescription.filePath));
        ARROW_ASSIGN_OR_RAISE(
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrow::csv::ReadOptions::Defaults(),
                        arrow::csv::ParseOptions::Defaults(),
                        arrow::csv::ConvertOptions::Defaults()
                ));

        std::shared_ptr<arrow::RecordBatch> currBatch;

        int blockIdx = 0;
        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++ it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    arrowPopulateColumnsAndCountUnstrPropertyListSizesTask<T>,
                    nodeTableSchema->primaryKeyPropertyIdx,
                    blockIdx, offsetStart, pkIndex.get(), this, currBatch));
            offsetStart += currBatch->num_rows();
            ++ blockIdx;
        }

        taskScheduler.waitAllTasksToCompleteOrError();
        logger->info("Flush the pk index to disk.");
        pkIndex->flush();
        logger->info("Done populating structured properties, constructing the pk index and counting "
                     "unstructured properties.");

        return arrow::Status::OK();
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

    template<typename T>
    arrow::Status InMemArrowNodeCSVCopier::arrowPopulateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
                                                                                             uint64_t blockId,
                                                                                             uint64_t startOffset,
                                                                                             HashIndexBuilder<T> *pkIndex,
                                                                                             InMemArrowNodeCSVCopier *copier,
                                                                                             std::shared_ptr<arrow::RecordBatch> currBatch) {
        copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
        auto arrow_columns = currBatch->columns();
        // TODO: Consider skip header
//        skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
        for (auto bufferOffset = 0u; bufferOffset < copier->numLinesPerBlock[blockId]; ++ bufferOffset) {
            putPropsOfLineIntoColumns(copier->structuredColumns,
                                      copier->nodeTableSchema->structuredProperties,
                                      overflowCursors,
                                      arrow_columns,
                                      startOffset + bufferOffset,
                                      bufferOffset);
        }
        populatePKIndex(copier->structuredColumns[IDColumnIdx].get(), pkIndex, startOffset,
                        copier->numLinesPerBlock[blockId]);
        copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        return arrow::Status::OK();
    }

    void InMemArrowNodeCSVCopier::calcLengthOfUnstrPropertyLists(
            CSVReader &reader, node_offset_t nodeOffset, InMemUnstructuredLists *unstrPropertyLists) {
        while (reader.hasNextToken()) {
            auto unstrPropertyString = reader.getString();
            auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
            if (!unstrPropertyStringBreaker1) {
                throw CopyCSVException("Unstructured property token in CSV is not in correct "
                                       "structure. It does not have ':' to separate"
                                       " the property key. token: " +
                                       string(unstrPropertyString));
            }
            *unstrPropertyStringBreaker1 = 0;
            auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
            if (!unstrPropertyStringBreaker2) {
                throw CopyCSVException("Unstructured property token in CSV is not in correct "
                                       "structure. It does not have ':' to separate"
                                       " the data type.");
            }
            *unstrPropertyStringBreaker2 = 0;
            auto dataType = Types::dataTypeFromString(string(unstrPropertyStringBreaker1 + 1));
            auto dataTypeSize = Types::getDataTypeSize(dataType);
            InMemListsUtils::incrementListSize(*unstrPropertyLists->getListSizes(), nodeOffset,
                                               StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        }
    }

    void InMemArrowNodeCSVCopier::calcUnstrListsHeadersAndMetadata() {
        // TODO(Semih): This can never be nullptr so we can remove this check.
        if (unstrPropertyLists == nullptr) {
            return;
        }
        logger->debug("Initializing UnstructuredPropertyListHeaderBuilders.");
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(calculateListHeadersTask,
                                                                         numNodes, 1,
                                                                         unstrPropertyLists->getListSizes(),
                                                                         unstrPropertyLists->getListHeadersBuilder(),
                                                                         logger));
        logger->debug("Done initializing UnstructuredPropertyListHeaders.");
        taskScheduler.waitAllTasksToCompleteOrError();
        logger->debug("Initializing UnstructuredPropertyListsMetadata.");
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes, 1,
                unstrPropertyLists->getListSizes(), unstrPropertyLists->getListHeadersBuilder(),
                unstrPropertyLists.get(), false /*hasNULLBytes*/, logger));
        logger->debug("Done initializing UnstructuredPropertyListsMetadata.");
        taskScheduler.waitAllTasksToCompleteOrError();
    }

    void InMemArrowNodeCSVCopier::populateUnstrPropertyLists() {
        logger->debug("Populating Unstructured Property Lists.");
        node_offset_t nodeOffsetStart = 0;
        for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    populateUnstrPropertyListsTask, blockIdx, nodeOffsetStart, this));
            nodeOffsetStart += numLinesPerBlock[blockIdx];
        }
        taskScheduler.waitAllTasksToCompleteOrError();
        logger->debug("Done populating Unstructured Property Lists.");
    }

    void InMemArrowNodeCSVCopier::populateUnstrPropertyListsTask(
            uint64_t blockId, node_offset_t nodeOffsetStart, InMemArrowNodeCSVCopier *copier) {
        copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        return;
    }

    void InMemArrowNodeCSVCopier::putPropsOfLineIntoColumns(
            vector<unique_ptr<InMemColumn>> &structuredColumns,
            const vector<Property> &structuredProperties, vector<PageByteCursor> &overflowCursors,
            std::vector<shared_ptr<arrow::Array>> &arrow_columns,
            uint64_t nodeOffset, uint64_t bufferOffset) {
        for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
            auto column = structuredColumns[columnIdx].get();

            // TODO: To improve efficiency, do not slice here. Slice before this function call

            auto currentTokenArray = arrow_columns[columnIdx]->Slice(bufferOffset);

            switch (column->getDataType().typeID) {
                case INT64: {
                    if (!arrow_columns[columnIdx]->IsNull(bufferOffset)) {
                        auto tokenIntArray = std::static_pointer_cast<arrow::Int64Array>(currentTokenArray);
                        int64_t int64Val = tokenIntArray->Value(0);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&int64Val));
                    }
                }
                    break;
                default:
                    break;
            }
        }
    }

    void InMemArrowNodeCSVCopier::putUnstrPropsOfALineToLists(CSVReader &reader, node_offset_t nodeOffset,
                                                              PageByteCursor &inMemOverflowFileCursor,
                                                              unordered_map<string, uint64_t> &unstrPropertiesNameToIdMap,
                                                              InMemUnstructuredLists *unstrPropertyLists) {
        while (reader.hasNextToken()) {
            auto unstrPropertyString = reader.getString();
            // Note: We do not check if the unstrPropertyString is in correct format below because
            // this was already done when inside populateColumnsAndCountUnstrPropertyListSizesTask,
            // when calling calcLengthOfUnstrPropertyLists. E.g., below we don't check if
            // unstrPropertyStringBreaker1 is null or not as we do in calcLengthOfUnstrPropertyLists.
            auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
            *unstrPropertyStringBreaker1 = 0;
            auto propertyKeyId = (uint32_t) unstrPropertiesNameToIdMap.at(string(unstrPropertyString));
            auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
            *unstrPropertyStringBreaker2 = 0;
            auto dataType = Types::dataTypeFromString(string(unstrPropertyStringBreaker1 + 1));
            auto dataTypeSize = Types::getDataTypeSize(dataType);
            auto reversePos = InMemListsUtils::decrementListSize(*unstrPropertyLists->getListSizes(),
                                                                 nodeOffset,
                                                                 StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize);
            PageElementCursor pageElementCursor = InMemListsUtils::calcPageElementCursor(
                    unstrPropertyLists->getListHeadersBuilder()->getHeader(nodeOffset), reversePos, 1,
                    nodeOffset, *unstrPropertyLists->getListsMetadataBuilder(), false /*hasNULLBytes*/);
            PageByteCursor pageCursor{pageElementCursor.pageIdx, pageElementCursor.elemPosInPage};
            char *valuePtr = unstrPropertyStringBreaker2 + 1;
            switch (dataType.typeID) {
                case INT64: {
                    auto intVal = TypeUtils::convertToInt64(valuePtr);
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               (uint8_t *) (&intVal), &inMemOverflowFileCursor);
                }
                    break;
                case DOUBLE: {
                    auto doubleVal = TypeUtils::convertToDouble(valuePtr);
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(&doubleVal),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                case BOOL: {
                    auto boolVal = TypeUtils::convertToBoolean(valuePtr);
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(&boolVal),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                case DATE: {
                    char *beginningOfDateStr = valuePtr;
                    date_t dateVal = Date::FromCString(beginningOfDateStr, strlen(beginningOfDateStr));
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(&dateVal),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                case TIMESTAMP: {
                    char *beginningOfTimestampStr = valuePtr;
                    timestamp_t timestampVal =
                            Timestamp::FromCString(beginningOfTimestampStr, strlen(beginningOfTimestampStr));
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(&timestampVal),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                case INTERVAL: {
                    char *beginningOfIntervalStr = valuePtr;
                    interval_t intervalVal =
                            Interval::FromCString(beginningOfIntervalStr, strlen(beginningOfIntervalStr));
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(&intervalVal),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                case STRING: {
                    unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                                                               reinterpret_cast<uint8_t *>(valuePtr),
                                                               &inMemOverflowFileCursor);
                }
                    break;
                default:
                    throw CopyCSVException("unsupported dataType while parsing unstructured property");
            }
        }
    }

    void InMemArrowNodeCSVCopier::saveToFile() {
        logger->debug("Writing node structured columns to disk.");
        assert(!structuredColumns.empty());
        for (auto &column: structuredColumns) {
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    [&](InMemColumn *x) { x->saveToFile(); }, column.get()));
        }
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                [&](InMemLists *x) { x->saveToFile(); }, unstrPropertyLists.get()));
        taskScheduler.waitAllTasksToCompleteOrError();
        logger->debug("Done writing node structured columns to disk.");
    }

} // namespace storage
} // namespace kuzu
