#include "include/in_mem_node_csv_copier.h"

#include "include/copy_csv_task.h"

#include "src/storage/storage_structure/include/in_mem_file.h"

namespace kuzu {
namespace storage {

InMemNodeCSVCopier::InMemNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
    : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
      numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
    nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
}

uint64_t InMemNodeCSVCopier::copy() {
    logger->info(
        "Copying node {} with table {}.", nodeTableSchema->tableName, nodeTableSchema->tableID);
    calculateNumBlocks(csvDescription.filePath, nodeTableSchema->tableName);
    auto unstructuredPropertyNames =
        countLinesPerBlockAndParseUnstrPropertyNames(nodeTableSchema->getNumStructuredProperties());
    catalog.setUnstructuredPropertiesOfNodeTableSchema(
        unstructuredPropertyNames, nodeTableSchema->tableID);
    numNodes = calculateNumRows(csvDescription.csvReaderConfig.hasHeader);
    initializeColumnsAndList();
    // Populate structured columns with the ID hash index and count the size of unstructured
    // lists.
    switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
    case INT64: {
        populateColumnsAndCountUnstrPropertyListSizes<int64_t>();
    } break;
    case STRING: {
        populateColumnsAndCountUnstrPropertyListSizes<ku_string_t>();
    } break;
    default: {
        throw CopyCSVException("Unsupported data type " +
                               Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                               " for the ID index.");
    }
    }
    calcUnstrListsHeadersAndMetadata();
    populateUnstrPropertyLists();
    saveToFile();
    nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
    logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
        nodeTableSchema->tableID);
    return numNodes;
}

void InMemNodeCSVCopier::initializeColumnsAndList() {
    logger->info("Initializing in memory structured columns and unstructured list.");
    structuredColumns.resize(nodeTableSchema->getNumStructuredProperties());
    for (auto& property : nodeTableSchema->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
            nodeTableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
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
    vector<unordered_set<string>>& unstructuredPropertyNamesPerBlock) {
    unordered_set<string> unstructuredPropertyNames;
    for (auto& unstructuredPropertiesInBlock : unstructuredPropertyNamesPerBlock) {
        for (auto& propertyName : unstructuredPropertiesInBlock) {
            unstructuredPropertyNames.insert(propertyName);
        }
    }
    // Ensure the same order in different platforms.
    vector<string> result{unstructuredPropertyNames.begin(), unstructuredPropertyNames.end()};
    sort(result.begin(), result.end());
    return result;
}

vector<string> InMemNodeCSVCopier::countLinesPerBlockAndParseUnstrPropertyNames(
    uint64_t numStructuredProperties) {
    logger->info("Counting number of lines and read unstructured property names in each block.");
    vector<unordered_set<string>> unstructuredPropertyNamesPerBlock{numBlocks};
    numLinesPerBlock.resize(numBlocks);
    for (uint64_t blockId = 0; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            countNumLinesAndUnstrPropertiesPerBlockTask, csvDescription.filePath, blockId, this,
            numStructuredProperties, &unstructuredPropertyNamesPerBlock[blockId]));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info(
        "Done counting number of lines and read unstructured property names  in each block.");
    return mergeUnstrPropertyNamesFromBlocks(unstructuredPropertyNamesPerBlock);
}

template<typename T>
void InMemNodeCSVCopier::populateColumnsAndCountUnstrPropertyListSizes() {
    logger->info("Populating structured properties and Counting unstructured properties.");
    auto pkIndex =
        make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                             nodeTableSchema->tableID, DBFileType::WAL_VERSION),
            nodeTableSchema->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numNodes);
    node_offset_t offsetStart = 0;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            populateColumnsAndCountUnstrPropertyListSizesTask<T>,
            nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, pkIndex.get(), this));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating structured properties, constructing the pk index and counting "
                 "unstructured properties.");
}

template<typename T>
void InMemNodeCSVCopier::addIDsToIndex(InMemColumn* column, HashIndexBuilder<T>* hashIndex,
    node_offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (is_same<T, int64_t>::value) {
            auto key = (int64_t*)column->getElement(offset);
            if (!hashIndex->append(*key, offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(to_string(*key)));
            }
        } else {
            auto element = (ku_string_t*)column->getElement(offset);
            auto key = column->getInMemOverflowFile()->readString(element);
            if (!hashIndex->append(key.c_str(), offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(key));
            }
        }
    }
}

template<typename T>
void InMemNodeCSVCopier::populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
    node_offset_t startOffset, uint64_t numValues) {
    addIDsToIndex(column, pkIndex, startOffset, numValues);
}

void InMemNodeCSVCopier::skipFirstRowIfNecessary(
    uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader) {
    if (0 == blockId && csvDescription.csvReaderConfig.hasHeader && reader.hasNextLine()) {
        reader.skipLine();
    }
}

template<typename T>
void InMemNodeCSVCopier::populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
    uint64_t blockId, uint64_t startOffset, HashIndexBuilder<T>* pkIndex,
    InMemNodeCSVCopier* copier) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        putPropsOfLineIntoColumns(copier->structuredColumns,
            copier->nodeTableSchema->structuredProperties, overflowCursors, reader,
            startOffset + bufferOffset);
        // TODO(Semih): Uncomment when enabling ad-hoc properties.
        //        calcLengthOfUnstrPropertyLists(
        //            reader, startOffset + bufferOffset, copier->unstrPropertyLists.get());
        bufferOffset++;
    }
    populatePKIndex(copier->structuredColumns[IDColumnIdx].get(), pkIndex, startOffset,
        copier->numLinesPerBlock[blockId]);
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemNodeCSVCopier::calcLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists) {
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

void InMemNodeCSVCopier::calcUnstrListsHeadersAndMetadata() {
    // TODO(Semih): This can never be nullptr so we can remove this check.
    if (unstrPropertyLists == nullptr) {
        return;
    }
    logger->debug("Initializing UnstructuredPropertyListHeaderBuilders.");
    taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(calculateListHeadersTask,
        numNodes, 1, unstrPropertyLists->getListSizes(),
        unstrPropertyLists->getListHeadersBuilder(), logger));
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

void InMemNodeCSVCopier::populateUnstrPropertyLists() {
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

void InMemNodeCSVCopier::populateUnstrPropertyListsTask(
    uint64_t blockId, node_offset_t nodeOffsetStart, InMemNodeCSVCopier* copier) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
    auto bufferOffset = 0u;
    PageByteCursor inMemOverflowFileCursor;
    auto unstrPropertiesNameToIdMap = copier->catalog.getWriteVersion()
                                          ->getNodeTableSchema(copier->nodeTableSchema->tableID)
                                          ->unstrPropertiesNameToIdMap;
    while (reader.hasNextLine()) {
        for (auto i = 0u; i < copier->nodeTableSchema->getNumStructuredProperties(); ++i) {
            reader.hasNextTokenOrError();
        }
        // TODO(Semih): Uncomment when enabling ad-hoc properties
        //        putUnstrPropsOfALineToLists(reader, nodeOffsetStart + bufferOffset,
        //        inMemOverflowFileCursor,
        //            unstrPropertiesNameToIdMap,
        //            reinterpret_cast<InMemUnstructuredLists*>(copier->unstrPropertyLists.get()));
        bufferOffset++;
    }
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemNodeCSVCopier::putPropsOfLineIntoColumns(
    vector<unique_ptr<InMemColumn>>& structuredColumns,
    const vector<Property>& structuredProperties, vector<PageByteCursor>& overflowCursors,
    CSVReader& reader, uint64_t nodeOffset) {
    for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
        reader.hasNextTokenOrError();
        auto column = structuredColumns[columnIdx].get();
        switch (column->getDataType().typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto int64Val = reader.getInt64();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&int64Val));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto kuStr =
                    column->getInMemOverflowFile()->copyString(strVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*column->getDataType().childType);
                auto kuList =
                    column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemNodeCSVCopier::putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
    PageByteCursor& inMemOverflowFileCursor,
    unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    InMemUnstructuredLists* unstrPropertyLists) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        // Note: We do not check if the unstrPropertyString is in correct format below because
        // this was already done when inside populateColumnsAndCountUnstrPropertyListSizesTask,
        // when calling calcLengthOfUnstrPropertyLists. E.g., below we don't check if
        // unstrPropertyStringBreaker1 is null or not as we do in calcLengthOfUnstrPropertyLists.
        auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
        *unstrPropertyStringBreaker1 = 0;
        auto propertyKeyId = (uint32_t)unstrPropertiesNameToIdMap.at(string(unstrPropertyString));
        auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
        *unstrPropertyStringBreaker2 = 0;
        auto dataType = Types::dataTypeFromString(string(unstrPropertyStringBreaker1 + 1));
        auto dataTypeSize = Types::getDataTypeSize(dataType);
        auto reversePos = InMemListsUtils::decrementListSize(*unstrPropertyLists->getListSizes(),
            nodeOffset, StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        PageElementCursor pageElementCursor = InMemListsUtils::calcPageElementCursor(
            unstrPropertyLists->getListHeadersBuilder()->getHeader(nodeOffset), reversePos, 1,
            nodeOffset, *unstrPropertyLists->getListsMetadataBuilder(), false /*hasNULLBytes*/);
        PageByteCursor pageCursor{pageElementCursor.pageIdx, pageElementCursor.elemPosInPage};
        char* valuePtr = unstrPropertyStringBreaker2 + 1;
        switch (dataType.typeID) {
        case INT64: {
            auto intVal = TypeUtils::convertToInt64(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                (uint8_t*)(&intVal), &inMemOverflowFileCursor);
        } break;
        case DOUBLE: {
            auto doubleVal = TypeUtils::convertToDouble(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&doubleVal), &inMemOverflowFileCursor);
        } break;
        case BOOL: {
            auto boolVal = TypeUtils::convertToBoolean(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&boolVal), &inMemOverflowFileCursor);
        } break;
        case DATE: {
            char* beginningOfDateStr = valuePtr;
            date_t dateVal = Date::FromCString(beginningOfDateStr, strlen(beginningOfDateStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&dateVal), &inMemOverflowFileCursor);
        } break;
        case TIMESTAMP: {
            char* beginningOfTimestampStr = valuePtr;
            timestamp_t timestampVal =
                Timestamp::FromCString(beginningOfTimestampStr, strlen(beginningOfTimestampStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&timestampVal), &inMemOverflowFileCursor);
        } break;
        case INTERVAL: {
            char* beginningOfIntervalStr = valuePtr;
            interval_t intervalVal =
                Interval::FromCString(beginningOfIntervalStr, strlen(beginningOfIntervalStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&intervalVal), &inMemOverflowFileCursor);
        } break;
        case STRING: {
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(valuePtr), &inMemOverflowFileCursor);
        } break;
        default:
            throw CopyCSVException("unsupported dataType while parsing unstructured property");
        }
    }
}

void InMemNodeCSVCopier::saveToFile() {
    logger->debug("Writing node structured columns to disk.");
    assert(!structuredColumns.empty());
    for (auto& column : structuredColumns) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            [&](InMemColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
        [&](InMemLists* x) { x->saveToFile(); }, unstrPropertyLists.get()));
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node structured columns to disk.");
}

} // namespace storage
} // namespace kuzu
