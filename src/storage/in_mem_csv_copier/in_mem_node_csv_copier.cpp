#include "include/in_mem_node_csv_copier.h"

#include "include/copy_csv_task.h"
#include "spdlog/spdlog.h"

#include "src/storage/storage_structure/include/in_mem_file.h"

namespace graphflow {
namespace storage {

InMemNodeCSVCopier::InMemNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
    : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
      numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
    nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
}

void InMemNodeCSVCopier::copy() {
    logger->info(
        "Copying node {} with table {}.", nodeTableSchema->tableName, nodeTableSchema->tableID);
    calculateNumBlocks(csvDescription.filePath, nodeTableSchema->tableName);
    auto adhocPropertyNames =
        countLinesPerBlockAndParseAdhocProperties(nodeTableSchema->getNumPredifinedProperties());
    catalog.setAdhocPropertiesOfNodeTableSchema(adhocPropertyNames, nodeTableSchema->tableID);
    numNodes = calculateNumRows(csvDescription.csvReaderConfig.hasHeader);
    initializeColumnsAndList();
    // Populate structured columns with the ID hash index and count the size of unstructured
    // lists.
    populateColumnsAndCountUnstrPropertyListSizes();
    calcUnstrListsHeadersAndMetadata();
    populateUnstrPropertyLists();
    saveToFile();
    nodesStatisticsAndDeletedIDs->setMaxNodeOffsetForTable(nodeTableSchema->tableID, numNodes - 1);
    logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
        nodeTableSchema->tableID);
}

void InMemNodeCSVCopier::initializeColumnsAndList() {
    logger->info("Initializing in memory structured columns and unstructured list.");
    structuredColumns.resize(nodeTableSchema->getNumPredifinedProperties());
    for (auto& property : nodeTableSchema->predefinedProperties) {
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

static vector<PropertyNameDataType> mergeUnstrPropertyNamesFromBlocks(
    vector<unordered_map<string, DataType>> adhocPropertiesPerBlock) {
    unordered_map<string, DataType> adhocPropertyNameDataTypes;
    for (auto& adhocProperties : adhocPropertiesPerBlock) {
        for (auto& [name, dataType] : adhocProperties) {
            if (adhocPropertyNameDataTypes.contains(name)) {
                assert(adhocPropertyNameDataTypes.at(name) == dataType);
            } else {
                adhocPropertyNameDataTypes.insert({name, dataType});
            }
        }
    }
    // Ensure the same order in different platforms.
    vector<PropertyNameDataType> result;
    for (auto& [name, dataType] : adhocPropertyNameDataTypes) {
        result.push_back(PropertyNameDataType{name, dataType});
    }
    sort(result.begin(), result.end(),
        [](const PropertyNameDataType& left, const PropertyNameDataType& right) {
            return left.name < right.name;
        });
    return result;
}

vector<PropertyNameDataType> InMemNodeCSVCopier::countLinesPerBlockAndParseAdhocProperties(
    uint64_t numPredefinedProperties) {
    logger->info("Counting number of lines and parse adhoc properties names in each block.");
    vector<unordered_map<string, DataType>> adhocPropertiesPerBlock{numBlocks};
    numLinesPerBlock.resize(numBlocks);
    for (uint64_t blockId = 0; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            countNumLinesAndAdhocPropertiesPerBlockTask, csvDescription.filePath, blockId, this,
            numPredefinedProperties, &adhocPropertiesPerBlock[blockId]));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info(
        "Done counting number of lines and read unstructured property names  in each block.");
    return mergeUnstrPropertyNamesFromBlocks(adhocPropertiesPerBlock);
}

void InMemNodeCSVCopier::populateColumnsAndCountUnstrPropertyListSizes() {
    logger->info("Populating structured properties and Counting unstructured properties.");
    auto IDIndex =
        make_unique<InMemHashIndex>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                        nodeTableSchema->tableID, DBFileType::WAL_VERSION),
            nodeTableSchema->getPrimaryKey().dataType);
    IDIndex->bulkReserve(numNodes);
    node_offset_t offsetStart = 0;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            populateColumnsAndCountUnstrPropertyListSizesTask,
            nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, IDIndex.get(), this));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Flush the pk index to disk.");
    IDIndex->flush();
    logger->info("Done populating structured properties, constructing the pk index and counting "
                 "unstructured properties.");
}

template<DataTypeID DT>
void InMemNodeCSVCopier::addIDsToIndex(
    InMemColumn* column, InMemHashIndex* hashIndex, node_offset_t startOffset, uint64_t numValues) {
    assert(DT == INT64 || DT == STRING);
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (DT == INT64) {
            auto key = *(int64_t*)column->getElement(offset);
            if (!hashIndex->append(key, offset)) {
                throw CopyCSVException("ID value " + to_string(key) +
                                       " violates the uniqueness constraint for the ID property.");
            }
        } else {
            auto strStoredInInMemOvfFile = ((gf_string_t*)column->getElement(offset));
            auto key = column->getInMemOverflowFile()->readString(strStoredInInMemOvfFile);
            if (!hashIndex->append(key.c_str(), offset)) {
                throw CopyCSVException("ID value  " + key +
                                       " violates the uniqueness constraint for the ID property.");
            }
        }
    }
}

void InMemNodeCSVCopier::populateIDIndex(
    InMemColumn* column, InMemHashIndex* IDIndex, node_offset_t startOffset, uint64_t numValues) {
    switch (column->getDataType().typeID) {
    case INT64: {
        addIDsToIndex<INT64>(column, IDIndex, startOffset, numValues);
    } break;
    case STRING: {
        addIDsToIndex<STRING>(column, IDIndex, startOffset, numValues);
    } break;
    default:
        throw CopyCSVException("Unsupported data type " +
                               Types::dataTypeToString(column->getDataType()) +
                               " for the ID index.");
    }
}

void InMemNodeCSVCopier::skipFirstRowIfNecessary(
    uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader) {
    if (0 == blockId && csvDescription.csvReaderConfig.hasHeader && reader.hasNextLine()) {
        reader.skipLine();
    }
}

void InMemNodeCSVCopier::populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
    uint64_t blockId, uint64_t startOffset, InMemHashIndex* IDIndex, InMemNodeCSVCopier* copier) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumPredifinedProperties());
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        putPropsOfLineIntoColumns(copier->structuredColumns,
            copier->nodeTableSchema->predefinedProperties, overflowCursors, reader,
            startOffset + bufferOffset);
        calcLengthOfUnstrPropertyLists(
            reader, startOffset + bufferOffset, copier->unstrPropertyLists.get());
        bufferOffset++;
    }
    populateIDIndex(copier->structuredColumns[IDColumnIdx].get(), IDIndex, startOffset,
        copier->numLinesPerBlock[blockId]);
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemNodeCSVCopier::calcLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto startPos = strchr(unstrPropertyString, ':') + 1;
        *strchr(startPos, ':') = 0;
        InMemListsUtils::incrementListSize(*unstrPropertyLists->getListSizes(), nodeOffset,
            StorageConfig::UNSTR_PROP_HEADER_LEN +
                Types::getDataTypeSize(Types::dataTypeFromString(string(startPos))));
    }
}

void InMemNodeCSVCopier::calcUnstrListsHeadersAndMetadata() {
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
                                          ->adhocPropertiesNameToIdMap;
    while (reader.hasNextLine()) {
        for (auto i = 0u; i < copier->nodeTableSchema->getNumPredifinedProperties(); ++i) {
            reader.hasNextToken();
        }
        putUnstrPropsOfALineToLists(reader, nodeOffsetStart + bufferOffset, inMemOverflowFileCursor,
            unstrPropertiesNameToIdMap,
            reinterpret_cast<InMemUnstructuredLists*>(copier->unstrPropertyLists.get()));
        bufferOffset++;
    }
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemNodeCSVCopier::putPropsOfLineIntoColumns(
    vector<unique_ptr<InMemColumn>>& structuredColumns,
    const vector<Property>& structuredProperties, vector<PageByteCursor>& overflowCursors,
    CSVReader& reader, uint64_t nodeOffset) {
    for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
        reader.hasNextToken();
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
                auto gfStr =
                    column->getInMemOverflowFile()->copyString(strVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&gfStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*column->getDataType().childType);
                auto gfList =
                    column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&gfList));
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
} // namespace graphflow
