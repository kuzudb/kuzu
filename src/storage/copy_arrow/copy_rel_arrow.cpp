#include "storage/copy_arrow/copy_rel_arrow.h"

#include "spdlog/spdlog.h"
#include "storage/copy_arrow/copy_task.h"

namespace kuzu {
namespace storage {

CopyRelArrow::CopyRelArrow(CopyDescription& copyDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog,
    map<table_id_t, node_offset_t> maxNodeOffsetsPerNodeTable, BufferManager* bufferManager,
    table_id_t tableID, RelsStatistics* relsStatistics)
    : CopyStructuresArrow{copyDescription, std::move(outputDirectory), taskScheduler, catalog},
      maxNodeOffsetsPerTable{std::move(maxNodeOffsetsPerNodeTable)}, relsStatistics{
                                                                         relsStatistics} {
    dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    startRelID = relsStatistics->getNextRelID(dummyReadOnlyTrx.get());
    relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
    for (auto& nodeTableID : relTableSchema->getAllNodeTableIDs()) {
        assert(!pkIndexes.contains(nodeTableID));
        pkIndexes[nodeTableID] = make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(this->outputDirectory, nodeTableID),
            catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID)->getPrimaryKey().dataType,
            *bufferManager, nullptr /* wal */);
    }
}

uint64_t CopyRelArrow::copy() {
    logger->info(
        "Copying rel {} with table {}.", relTableSchema->tableName, relTableSchema->tableID);
    logger->info(
        "Reading " + CopyDescription::getFileTypeName(copyDescription.fileType) + " file.");

    countNumLines(copyDescription.filePath);
    initializeColumnsAndLists();

    // Construct columns and lists.
    populateAdjColumnsAndCountRelsInAdjLists();
    if (!directionTableAdjLists[FWD].empty() || !directionTableAdjLists[BWD].empty()) {
        initAdjListsHeaders();
        initListsMetadata();
        populateLists();
    }
    sortAndCopyOverflowValues();
    saveToFile();
    relsStatistics->setNumRelsForTable(relTableSchema->tableID, numRows);
    logger->info(
        "Done copying rel {} with table {}.", relTableSchema->tableName, relTableSchema->tableID);
    return numRows;
}

void CopyRelArrow::initializeColumnsAndLists() {
    directionNumRelsPerTable.resize(2);
    directionTableListSizes.resize(2);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& boundTableID : catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                 relTableSchema->tableID, relDirection)) {
            // Note we cannot do:
            // directionNumRelsPerTable[relDirection][tableIDSchema.first] = atomic<uint64_t>(0);
            // because atomic values are not movable.
            directionNumRelsPerTable[relDirection].emplace(boundTableID, 0);
            directionTableListSizes[relDirection][boundTableID] = make_unique<atomic_uint64_vec_t>(
                maxNodeOffsetsPerTable.at(boundTableID) + 1 /* num nodes */);
            directionNodeIDCompressionScheme[relDirection][boundTableID] = NodeIDCompressionScheme(
                relTableSchema->getUniqueNbrTableIDsForBoundTableIDDirection(
                    relDirection, boundTableID));
        }
        if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relTableSchema->tableID, relDirection)) {
            initializeColumns(relDirection);
        } else {
            initializeLists(relDirection);
        }
    }
    for (auto& property : relTableSchema->properties) {
        if (property.dataType.typeID == LIST || property.dataType.typeID == STRING) {
            overflowFilePerPropertyID[property.propertyID] = make_unique<InMemOverflowFile>();
        }
    }
}

void CopyRelArrow::initializeColumns(RelDirection relDirection) {
    auto boundTableIDs = catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
        relTableSchema->tableID, relDirection);
    for (auto boundTableID : boundTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
        directionTableAdjColumns[relDirection].emplace(boundTableID,
            make_unique<InMemAdjColumn>(
                StorageUtils::getAdjColumnFName(outputDirectory, relTableSchema->tableID,
                    boundTableID, relDirection, DBFileType::WAL_VERSION),
                directionNodeIDCompressionScheme[relDirection][boundTableID], numNodes));
        vector<unique_ptr<InMemColumn>> propertyColumns(relTableSchema->getNumProperties());
        for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
            auto propertyID = relTableSchema->properties[i].propertyID;
            auto propertyDataType = relTableSchema->properties[i].dataType;
            auto fName =
                StorageUtils::getRelPropertyColumnFName(outputDirectory, relTableSchema->tableID,
                    boundTableID, relDirection, propertyID, DBFileType::WAL_VERSION);
            propertyColumns[i] =
                InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes);
        }
        directionTablePropertyColumns[relDirection].emplace(
            boundTableID, std::move(propertyColumns));
    }
}

void CopyRelArrow::initializeLists(RelDirection relDirection) {
    auto boundTableIDs = catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
        relTableSchema->tableID, relDirection);
    for (auto boundTableID : boundTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
        directionTableAdjLists[relDirection].emplace(boundTableID,
            make_unique<InMemAdjLists>(
                StorageUtils::getAdjListsFName(outputDirectory, relTableSchema->tableID,
                    boundTableID, relDirection, DBFileType::WAL_VERSION),
                directionNodeIDCompressionScheme[relDirection][boundTableID], numNodes));
        vector<unique_ptr<InMemLists>> propertyLists(relTableSchema->getNumProperties());
        for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
            auto propertyName = relTableSchema->properties[i].name;
            auto propertyDataType = relTableSchema->properties[i].dataType;
            auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory,
                relTableSchema->tableID, boundTableID, relDirection,
                relTableSchema->properties[i].propertyID, DBFileType::WAL_VERSION);
            propertyLists[i] =
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
        }
        directionTablePropertyLists[relDirection].emplace(boundTableID, std::move(propertyLists));
    }
}

arrow::Status CopyRelArrow::executePopulateTask(PopulateTaskType populateTaskType) {
    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV:
        status = populateFromCSV(populateTaskType);
        break;

    case CopyDescription::FileType::ARROW:
        status = populateFromArrow(populateTaskType);
        break;

    case CopyDescription::FileType::PARQUET:
        status = populateFromParquet(populateTaskType);
        break;
    }
    return status;
}

void CopyRelArrow::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info(
        "Populating adj columns and rel property columns for rel {}.", relTableSchema->tableName);
    auto status =
        executePopulateTask(PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask);
    throwCopyExceptionIfNotOK(status);
    relsStatistics->setNumRelsPerDirectionBoundTableID(
        relTableSchema->tableID, directionNumRelsPerTable);
    logger->info("Done populating adj columns and rel property columns for rel {}.",
        relTableSchema->tableName);
}

arrow::Status CopyRelArrow::populateFromCSV(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::Array>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::Array>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
    auto status = initCSVReader(csv_streaming_reader, copyDescription.filePath);
    throwCopyExceptionIfNotOK(status);
    std::shared_ptr<arrow::RecordBatch> currBatch;
    int blockIdx = 0;
    auto blockStartOffset = 0ull;
    auto it = csv_streaming_reader->begin();
    auto endIt = csv_streaming_reader->end();
    while (it != endIt) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (it == endIt) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(populateTask, blockIdx,
                startRelID + blockStartOffset, this, currBatch->columns(), copyDescription));
            blockStartOffset += numLinesPerBlock[blockIdx];
            ++it;
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

arrow::Status CopyRelArrow::populateFromArrow(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::Array>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::Array>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReader(ipc_reader, copyDescription.filePath);
    throwCopyExceptionIfNotOK(status);
    std::shared_ptr<arrow::RecordBatch> currBatch;
    int blockIdx = 0;
    auto blockStartOffset = 0ull;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, ipc_reader->ReadRecordBatch(blockIdx));
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(populateTask, blockIdx,
                startRelID + blockStartOffset, this, currBatch->columns(), copyDescription));
            blockStartOffset += numLinesPerBlock[blockIdx];
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

arrow::Status CopyRelArrow::populateFromParquet(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::ChunkedArray>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::ChunkedArray>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReader(reader, copyDescription.filePath);
    throwCopyExceptionIfNotOK(status);
    std::shared_ptr<arrow::Table> currTable;
    int blockIdx = 0;
    auto blockStartOffset = 0ull;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(populateTask, blockIdx,
                startRelID + blockStartOffset, this, currTable->columns(), copyDescription));
            blockStartOffset += numLinesPerBlock[blockIdx];
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

static void putValueIntoColumns(uint64_t propertyIdx,
    vector<table_property_in_mem_columns_map_t>& directionTablePropertyColumns,
    const vector<nodeID_t>& nodeIDs, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto tableID = nodeIDs[relDirection].tableID;
        if (directionTablePropertyColumns[relDirection].contains(tableID)) {
            auto propertyColumn =
                directionTablePropertyColumns[relDirection].at(tableID)[propertyIdx].get();
            auto nodeOffset = nodeIDs[relDirection].offset;
            propertyColumn->setElement(nodeOffset, val);
        }
    }
}

template<typename T>
void CopyRelArrow::populateAdjColumnsAndCountRelsInAdjListsTask(uint64_t blockId,
    uint64_t blockStartRelID, CopyRelArrow* copier, const vector<shared_ptr<T>>& batchColumns,
    CopyDescription& copyDescription) {
    copier->logger->debug(
        "Start: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
    vector<bool> requireToReadTableLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodePKTypes{2};
    for (auto& relDirection : REL_DIRECTIONS) {
        auto nodeTableIDs =
            copier->catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                copier->relTableSchema->tableID, relDirection);
        requireToReadTableLabels[relDirection] = nodeTableIDs.size() > 1;
        nodeIDs[relDirection].tableID = *nodeTableIDs.begin();
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(nodeIDs[relDirection].tableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    vector<PageByteCursor> inMemOverflowFileCursors{copier->relTableSchema->getNumProperties()};
    uint64_t relID = blockStartRelID;
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, requireToReadTableLabels, blockOffset,
            colIndex);
        for (auto relDirection : REL_DIRECTIONS) {
            auto tableID = nodeIDs[relDirection].tableID;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (copier->directionTableAdjColumns[relDirection].contains(tableID)) {
                if (!copier->directionTableAdjColumns[relDirection].at(tableID)->isNullAtNodeOffset(
                        nodeOffset)) {
                    auto relTableSchema = copier->relTableSchema;
                    throw CopyException(StringUtils::string_format(
                        "RelTable %s is a %s table, but node(nodeOffset: %d, tableName: %s) has "
                        "more than one neighbour in the %s direction.",
                        relTableSchema->tableName.c_str(),
                        getRelMultiplicityAsString(relTableSchema->relMultiplicity).c_str(),
                        nodeOffset,
                        copier->catalog.getReadOnlyVersion()->getTableName(tableID).c_str(),
                        getRelDirectionAsString(relDirection).c_str()));
                }
                copier->directionTableAdjColumns[relDirection].at(tableID)->setElement(
                    nodeOffset, (uint8_t*)&nodeIDs[!relDirection]);
            } else {
                InMemListsUtils::incrementListSize(
                    *copier->directionTableListSizes[relDirection].at(tableID), nodeOffset, 1);
            }
            copier->directionNumRelsPerTable[relDirection].at(tableID)++;
        }
        if (copier->relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoColumns<T>(copier, inMemOverflowFileCursors, batchColumns, nodeIDs,
                blockOffset, colIndex, copyDescription);
        }
        putValueIntoColumns(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->directionTablePropertyColumns, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
}

template<typename T>
void CopyRelArrow::putPropsOfLineIntoColumns(CopyRelArrow* copier,
    vector<PageByteCursor>& inMemOverflowFileCursors, const vector<shared_ptr<T>>& batchColumns,
    const vector<nodeID_t>& nodeIDs, int64_t blockOffset, int64_t& colIndex,
    CopyDescription& copyDescription) {
    auto& properties = copier->relTableSchema->properties;
    auto& directionTablePropertyColumns = copier->directionTablePropertyColumns;
    auto& inMemOverflowFilePerPropertyID = copier->overflowFilePerPropertyID;
    for (auto propertyIdx = RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX + 1;
         propertyIdx < properties.size(); propertyIdx++) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto currentToken = batchColumns[colIndex]->GetScalar(blockOffset);
        if (!(*currentToken)->is_valid) {
            ++colIndex;
            continue;
        }
        auto stringToken = currentToken->get()->ToString().substr(0, DEFAULT_PAGE_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            int64_t val = TypeUtils::convertToInt64(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case DOUBLE: {
            double_t val = TypeUtils::convertToDouble(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case BOOL: {
            bool val = TypeUtils::convertToBoolean(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case DATE: {
            date_t val = Date::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case TIMESTAMP: {
            timestamp_t val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case INTERVAL: {
            interval_t val = Interval::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case STRING: {
            auto kuStr = inMemOverflowFilePerPropertyID[propertyIdx]->copyString(
                data, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case LIST: {
            auto listVal = getArrowList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copyDescription);
            auto kuList = inMemOverflowFilePerPropertyID[propertyIdx]->copyList(
                *listVal, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuList));
        } break;
        default:
            break;
        }
        ++colIndex;
    }
}

template<typename T>
void CopyRelArrow::inferTableIDsAndOffsets(const vector<shared_ptr<T>>& batchColumns,
    vector<nodeID_t>& nodeIDs, vector<DataType>& nodeIDTypes,
    const map<table_id_t, unique_ptr<PrimaryKeyIndex>>& pkIndexes, Transaction* transaction,
    const Catalog& catalog, vector<bool> requireToReadTableLabels, int64_t blockOffset,
    int64_t& colIndex) {
    for (auto& relDirection : REL_DIRECTIONS) {
        if (requireToReadTableLabels[relDirection]) {
            if (colIndex >= batchColumns.size()) {
                throw CopyException("Number of columns mismatch.");
            }
            auto nodeTableName = batchColumns[colIndex]->GetScalar(blockOffset)->get()->ToString();
            if (!catalog.getReadOnlyVersion()->containNodeTable(nodeTableName)) {
                throw CopyException("NodeTableName: " + nodeTableName + " does not exist.");
            }
            nodeIDs[relDirection].tableID = catalog.getReadOnlyVersion()->getTableID(nodeTableName);
            ++colIndex;
        }
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto keyToken = batchColumns[colIndex]->GetScalar(blockOffset)->get()->ToString();
        auto keyStr = keyToken.c_str();
        ++colIndex;
        switch (nodeIDTypes[relDirection].typeID) {
        case INT64: {
            auto key = TypeUtils::convertToInt64(keyStr);
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, key, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + to_string(key) + " in the pkIndex.");
            }
        } break;
        case STRING: {
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + string(keyStr) + " in the pkIndex.");
            }
        } break;
        default:
            throw CopyException("Unsupported data type " +
                                Types::dataTypeToString(nodeIDTypes[relDirection]) +
                                " for index lookup.");
        }
    }
}

static void putValueIntoLists(uint64_t propertyIdx,
    vector<table_property_in_mem_lists_map_t>& directionTablePropertyLists,
    vector<table_adj_in_mem_lists_map_t>& directionTableAdjLists, const vector<nodeID_t>& nodeIDs,
    const vector<uint64_t>& reversePos, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto tableID = nodeIDs[relDirection].tableID;
        if (directionTablePropertyLists[relDirection].contains(tableID)) {
            auto propertyList =
                directionTablePropertyLists[relDirection].at(tableID)[propertyIdx].get();
            auto nodeOffset = nodeIDs[relDirection].offset;
            auto header =
                directionTableAdjLists[relDirection][tableID]->getListHeadersBuilder()->getHeader(
                    nodeOffset);
            propertyList->setElement(header, nodeOffset, reversePos[relDirection], val);
        }
    }
}

template<typename T>
void CopyRelArrow::putPropsOfLineIntoLists(CopyRelArrow* copier,
    vector<PageByteCursor>& inMemOverflowFileCursors, const vector<shared_ptr<T>>& batchColumns,
    const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos, int64_t blockOffset,
    int64_t& colIndex, CopyDescription& copyDescription) {
    auto& properties = copier->relTableSchema->properties;
    auto& directionTablePropertyLists = copier->directionTablePropertyLists;
    auto& directionTableAdjLists = copier->directionTableAdjLists;
    auto& inMemOverflowFilesPerProperty = copier->overflowFilePerPropertyID;
    for (auto propertyIdx = RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX + 1;
         propertyIdx < properties.size(); propertyIdx++) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto currentToken = batchColumns[colIndex]->GetScalar(blockOffset);
        if (!(*currentToken)->is_valid) {
            ++colIndex;
            continue;
        }
        auto stringToken = currentToken->get()->ToString().substr(0, DEFAULT_PAGE_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            int64_t val = TypeUtils::convertToInt64(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case DOUBLE: {
            double_t val = TypeUtils::convertToDouble(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case BOOL: {
            bool val = TypeUtils::convertToBoolean(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case DATE: {
            date_t val = Date::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case TIMESTAMP: {
            timestamp_t val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case INTERVAL: {
            interval_t val = Interval::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case STRING: {
            auto kuStr = inMemOverflowFilesPerProperty[propertyIdx]->copyString(
                data, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case LIST: {
            auto listVal = getArrowList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copyDescription);
            auto kuList = inMemOverflowFilesPerProperty[propertyIdx]->copyList(
                *listVal, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuList));
        } break;
        default:
            break;
        }
        ++colIndex;
    }
}

void CopyRelArrow::initAdjListsHeaders() {
    logger->debug("Initializing AdjListHeaders for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [boundTableID, adjList] : directionTableAdjLists[relDirection]) {
            auto numBytesPerNode = directionNodeIDCompressionScheme[relDirection][boundTableID]
                                       .getNumBytesForNodeIDAfterCompression();
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(calculateListHeadersTask,
                maxNodeOffsetsPerTable.at(boundTableID) + 1, numBytesPerNode,
                directionTableListSizes[relDirection][boundTableID].get(),
                adjList->getListHeadersBuilder(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", relTableSchema->tableName);
}

void CopyRelArrow::initListsMetadata() {
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [boundTableID, adjList] : directionTableAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto listSizes = directionTableListSizes[relDirection][boundTableID].get();
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                directionNodeIDCompressionScheme[relDirection][boundTableID]
                    .getNumBytesForNodeIDAfterCompression(),
                listSizes, adjList->getListHeadersBuilder(), adjList.get(), false /*hasNULLBytes*/,
                logger));
            for (auto& property : relTableSchema->properties) {
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                    Types::getDataTypeSize(property.dataType), listSizes,
                    adjList->getListHeadersBuilder(),
                    directionTablePropertyLists[relDirection][boundTableID][property.propertyID]
                        .get(),
                    true /*hasNULLBytes*/, logger));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing adjLists and propertyLists metadata for rel {}.",
        relTableSchema->tableName);
}

void CopyRelArrow::populateLists() {
    logger->debug(
        "Populating adjLists and rel property lists for rel {}.", relTableSchema->tableName);
    auto status = executePopulateTask(PopulateTaskType::populateListsTask);
    throwCopyExceptionIfNotOK(status);
    logger->debug(
        "Done populating adjLists and rel property lists for rel {}.", relTableSchema->tableName);
}

template<typename T>
void CopyRelArrow::populateListsTask(uint64_t blockId, uint64_t blockStartRelID,
    CopyRelArrow* copier, const vector<shared_ptr<T>>& batchColumns,
    CopyDescription& copyDescription) {
    copier->logger->trace(
        "Start: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
    vector<bool> requireToReadTableLabels{true, true};
    vector<nodeID_t> nodeIDs(2);
    vector<DataType> nodePKTypes(2);
    vector<uint64_t> reversePos(2);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeTableIDs =
            copier->catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                copier->relTableSchema->tableID, relDirection);
        requireToReadTableLabels[relDirection] = nodeTableIDs.size() != 1;
        nodeIDs[relDirection].tableID = *nodeTableIDs.begin();
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(nodeIDs[relDirection].tableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    vector<PageByteCursor> inMemOverflowFileCursors(copier->relTableSchema->getNumProperties());
    uint64_t relID = blockStartRelID;
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, requireToReadTableLabels, blockOffset,
            colIndex);
        for (auto relDirection : REL_DIRECTIONS) {
            if (!copier->catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    copier->relTableSchema->tableID, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto tableID = nodeIDs[relDirection].tableID;
                auto adjList = copier->directionTableAdjLists[relDirection][tableID].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *copier->directionTableListSizes[relDirection][tableID],
                    nodeIDs[relDirection].offset, 1);
                adjList->setElement(adjList->getListHeadersBuilder()->getHeader(nodeOffset),
                    nodeOffset, reversePos[relDirection], (uint8_t*)(&nodeIDs[!relDirection]));
            }
        }
        if (copier->relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoLists<T>(copier, inMemOverflowFileCursors, batchColumns, nodeIDs,
                reversePos, blockOffset, colIndex, copyDescription);
        }
        putValueIntoLists(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->directionTablePropertyLists, copier->directionTableAdjLists, nodeIDs,
            reversePos, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->trace("End: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
}

void CopyRelArrow::copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
    PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
    InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile) {
    if (kuStr->len > ku_string_t::SHORT_STR_LENGTH) {
        TypeUtils::decodeOverflowPtr(kuStr->overflowPtr, unorderedOverflowCursor.pageIdx,
            unorderedOverflowCursor.offsetInPage);
        orderedOverflowFile->copyStringOverflow(orderedOverflowCursor,
            unorderedOverflowFile->getPage(unorderedOverflowCursor.pageIdx)->data +
                unorderedOverflowCursor.offsetInPage,
            kuStr);
    }
}

void CopyRelArrow::copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
    const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        kuList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflowFromFile(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, kuList, dataType.childType.get());
}

void CopyRelArrow::sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
    InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        if (dataType.typeID == STRING) {
            auto kuStr = reinterpret_cast<ku_string_t*>(propertyColumn->getElement(offsetStart));
            copyStringOverflowFromUnorderedToOrderedPages(kuStr, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else if (dataType.typeID == LIST) {
            auto kuList = reinterpret_cast<ku_list_t*>(propertyColumn->getElement(offsetStart));
            copyListOverflowFromUnorderedToOrderedPages(kuList, dataType, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else {
            assert(false);
        }
    }
}

void CopyRelArrow::sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
    InMemLists* propertyLists, InMemOverflowFile* unorderedInMemOverflowFile,
    InMemOverflowFile* orderedInMemOverflowFile) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    PageElementCursor propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        auto header = adjLists->getListHeadersBuilder()->getHeader(offsetStart);
        uint32_t listsLen =
            ListHeaders::isALargeList(header) ?
                propertyLists->getListsMetadataBuilder()->getNumElementsInLargeLists(
                    ListHeaders::getLargeListIdx(header)) :
                ListHeaders::getSmallListLen(header);
        for (auto pos = listsLen; pos > 0; pos--) {
            propertyListCursor = InMemListsUtils::calcPageElementCursor(header, pos,
                Types::getDataTypeSize(dataType), offsetStart,
                *propertyLists->getListsMetadataBuilder(), true /*hasNULLBytes*/);
            if (dataType.typeID == STRING) {
                auto kuStr = reinterpret_cast<ku_string_t*>(propertyLists->getMemPtrToLoc(
                    propertyListCursor.pageIdx, propertyListCursor.elemPosInPage));
                copyStringOverflowFromUnorderedToOrderedPages(kuStr, unorderedOverflowCursor,
                    orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
            } else if (dataType.typeID == LIST) {
                auto kuList = reinterpret_cast<ku_list_t*>(propertyLists->getMemPtrToLoc(
                    propertyListCursor.pageIdx, propertyListCursor.elemPosInPage));
                copyListOverflowFromUnorderedToOrderedPages(kuList, dataType,
                    unorderedOverflowCursor, orderedOverflowCursor, unorderedInMemOverflowFile,
                    orderedInMemOverflowFile);
            } else {
                assert(false);
            }
        }
    }
}

void CopyRelArrow::sortAndCopyOverflowValues() {
    for (auto relDirection : REL_DIRECTIONS) {
        // Sort overflow values of property Lists.
        for (auto& [tableID, adjList] : directionTableAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerTable.at(tableID) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relTableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyList = directionTablePropertyLists[relDirection]
                                                .at(tableID)[property.propertyID]
                                                .get();
                        taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                            sortOverflowValuesOfPropertyListsTask, property.dataType, offsetStart,
                            offsetEnd, adjList.get(), propertyList,
                            overflowFilePerPropertyID.at(property.propertyID).get(),
                            propertyList->getInMemOverflowFile()));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    // Sort overflow values of property columns.
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [tableID, column] : directionTablePropertyColumns[relDirection]) {
            auto numNodes = maxNodeOffsetsPerTable.at(tableID) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relTableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyColumn = directionTablePropertyColumns[relDirection]
                                                  .at(tableID)[property.propertyID]
                                                  .get();
                        taskScheduler.scheduleTask(
                            CopyTaskFactory::createCopyTask(sortOverflowValuesOfPropertyColumnTask,
                                property.dataType, offsetStart, offsetEnd, propertyColumn,
                                overflowFilePerPropertyID.at(property.propertyID).get(),
                                propertyColumn->getInMemOverflowFile()));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    overflowFilePerPropertyID.clear();
}

void CopyRelArrow::saveToFile() {
    logger->debug("Writing columns and Lists to disk for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        // Write columns
        for (auto& [_, adjColumn] : directionTableAdjColumns[relDirection]) {
            adjColumn->saveToFile();
        }
        for (auto& [_, propertyColumns] : directionTablePropertyColumns[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < relTableSchema->getNumProperties();
                 propertyIdx++) {
                propertyColumns[propertyIdx]->saveToFile();
            }
        }
        for (auto& [_, adjList] : directionTableAdjLists[relDirection]) {
            adjList->saveToFile();
        }
        for (auto& [_, propertyLists] : directionTablePropertyLists[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < relTableSchema->getNumProperties();
                 propertyIdx++) {
                propertyLists[propertyIdx]->saveToFile();
            }
        }
    }
    logger->debug("Done writing columns and lists to disk for rel {}.", relTableSchema->tableName);
}

std::string CopyRelArrow::getTaskTypeName(PopulateTaskType populateTaskType) {
    switch (populateTaskType) {
    case PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask:
        return "populateAdjColumnsAndCountRelsInAdjListsTask";

    case PopulateTaskType::populateListsTask:
        return "populateListsTask";
    }
}

} // namespace storage
} // namespace kuzu
