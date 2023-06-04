#include "storage/copier/rel_copy_executor.h"

#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "storage/copier/copy_task.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelCopyExecutor::RelCopyExecutor(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, storage::NodesStore& nodesStore,
    RelTable* table, RelsStatistics* relsStatistics)
    : TableCopyExecutor{copyDescription, std::move(outputDirectory), taskScheduler, catalog,
          table->getRelTableID(), relsStatistics},
      nodesStore{nodesStore}, table{table},
      maxNodeOffsetsPerTable{
          nodesStore.getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable()} {
    dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    initializePkIndexes(relTableSchema->srcTableID);
    initializePkIndexes(relTableSchema->dstTableID);
}

std::string RelCopyExecutor::getTaskTypeName(PopulateTaskType populateTaskType) {
    switch (populateTaskType) {
    case PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask: {
        return "populateAdjColumnsAndCountRelsInAdjListsTask";
    }
    case PopulateTaskType::populateListsTask: {
        return "populateListsTask";
    }
    }
}

void RelCopyExecutor::initializeColumnsAndLists() {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        listSizesPerDirection[relDirection] = std::make_unique<atomic_uint64_vec_t>(
            maxNodeOffsetsPerTable.at(
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection)) +
            1);
        if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                tableSchema->tableID, relDirection)) {
            initializeColumns(relDirection);
        } else {
            initializeLists(relDirection);
        }
    }
    for (auto& property : tableSchema->properties) {
        if (property.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
            property.dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
            overflowFilePerPropertyID[property.propertyID] = std::make_unique<InMemOverflowFile>();
        }
    }
}

void RelCopyExecutor::populateColumnsAndLists(processor::ExecutionContext* executionContext) {
    populateAdjColumnsAndCountRelsInAdjLists();
    if (adjListsPerDirection[FWD] != nullptr || adjListsPerDirection[BWD] != nullptr) {
        initAdjListsHeaders();
        initListsMetadata();
        populateLists();
    }
    sortAndCopyOverflowValues();
}

void RelCopyExecutor::saveToFile() {
    logger->debug("Writing columns and Lists to disk for rel {}.", tableSchema->tableName);
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (reinterpret_cast<RelTableSchema*>(tableSchema)
                ->isSingleMultiplicityInDirection(relDirection)) {
            adjColumnsPerDirection[relDirection]->flushChunk(
                adjColumnChunksPerDirection[relDirection].get());
            adjColumnsPerDirection[relDirection]->saveToFile();
            for (auto& [propertyID, propertyColumn] : propertyColumnsPerDirection[relDirection]) {
                propertyColumn->flushChunk(
                    propertyColumnChunksPerDirection[relDirection].at(propertyID).get());
                propertyColumn->saveToFile();
            }
        } else {
            adjListsPerDirection[relDirection]->saveToFile();
            for (auto& [_, propertyLists] : propertyListsPerDirection[relDirection]) {
                propertyLists->saveToFile();
            }
        }
    }
    logger->debug("Done writing columns and lists to disk for rel {}.", tableSchema->tableName);
}

void RelCopyExecutor::initializeColumns(RelDataDirection relDirection) {
    auto boundTableID =
        reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
    adjColumnsPerDirection[relDirection] = std::make_unique<InMemColumn>(
        StorageUtils::getAdjColumnFName(
            outputDirectory, tableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        LogicalType(LogicalTypeID::INTERNAL_ID));
    adjColumnChunksPerDirection[relDirection] =
        adjColumnsPerDirection[relDirection]->getInMemColumnChunk(
            0, numNodes - 1, &copyDescription);
    std::unordered_map<property_id_t, std::unique_ptr<InMemColumn>> propertyColumns;
    std::unordered_map<property_id_t, std::unique_ptr<InMemColumnChunk>> propertyColumnChunks;
    for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
        auto propertyID = tableSchema->properties[i].propertyID;
        auto propertyDataType = tableSchema->properties[i].dataType;
        auto fName = StorageUtils::getRelPropertyColumnFName(outputDirectory, tableSchema->tableID,
            relDirection, propertyID, DBFileType::WAL_VERSION);
        propertyColumns.emplace(propertyID, std::make_unique<InMemColumn>(fName, propertyDataType));
        propertyColumnChunks.emplace(
            propertyID, propertyColumns[i]->getInMemColumnChunk(0, numNodes - 1, &copyDescription));
    }
    propertyColumnsPerDirection[relDirection] = std::move(propertyColumns);
    propertyColumnChunksPerDirection[relDirection] = std::move(propertyColumnChunks);
}

void RelCopyExecutor::initializeLists(RelDataDirection relDirection) {
    auto boundTableID =
        reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
    adjListsPerDirection[relDirection] = std::make_unique<InMemAdjLists>(
        StorageUtils::getAdjListsFName(
            outputDirectory, tableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        numNodes);
    std::unordered_map<property_id_t, std::unique_ptr<InMemLists>> propertyLists;
    for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
        auto propertyID = tableSchema->properties[i].propertyID;
        auto propertyDataType = tableSchema->properties[i].dataType;
        auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory, tableSchema->tableID,
            relDirection, propertyID, DBFileType::WAL_VERSION);
        propertyLists.emplace(
            propertyID, InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes,
                            adjListsPerDirection[relDirection]->getListHeadersBuilder()));
    }
    propertyListsPerDirection[relDirection] = std::move(propertyLists);
}

void RelCopyExecutor::initAdjListsHeaders() {
    // TODO(Semih): Schedule one at a time and wait.
    logger->debug("Initializing AdjListHeaders for rel {}.", tableSchema->tableName);
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(calculateListHeadersTask,
                maxNodeOffsetsPerTable.at(boundTableID) + 1,
                listSizesPerDirection[relDirection].get(),
                adjListsPerDirection[relDirection]->getListHeadersBuilder().get(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", tableSchema->tableName);
}

void RelCopyExecutor::initListsMetadata() {
    // TODO(Semih): Schedule one at a time and wait.
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", tableSchema->tableName);
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            auto adjLists = adjListsPerDirection[relDirection].get();
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto listSizes = listSizesPerDirection[relDirection].get();
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                (uint32_t)sizeof(offset_t), listSizes, adjLists->getListHeadersBuilder().get(),
                adjLists, false /*hasNULLBytes*/, logger));
            for (auto& property : tableSchema->properties) {
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                    storage::StorageUtils::getDataTypeSize(property.dataType), listSizes,
                    adjLists->getListHeadersBuilder().get(),
                    propertyListsPerDirection[relDirection][property.propertyID].get(),
                    true /*hasNULLBytes*/, logger));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing adjLists and propertyLists metadata for rel {}.",
        tableSchema->tableName);
}

void RelCopyExecutor::executePopulateTask(PopulateTaskType populateTaskType) {
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        populateFromCSV(populateTaskType);
    } break;
    case CopyDescription::FileType::PARQUET: {
        populateFromParquet(populateTaskType);
    } break;
    default: {
        throw CopyException(StringUtils::string_format("Unsupported file type {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType)));
    }
    }
}

void RelCopyExecutor::populateFromCSV(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::Array>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::Array>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    for (auto& filePath : copyDescription.filePaths) {
        offset_t startOffset = fileBlockInfos.at(filePath).startOffset;
        auto reader = createCSVReader(filePath, copyDescription.csvReaderConfig.get(), tableSchema);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        int blockIdx = 0;
        while (true) {
            for (auto i = 0u; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; i++) {
                throwCopyExceptionIfNotOK(reader->ReadNext(&currBatch));
                if (currBatch == nullptr) {
                    // No more batches left, thus, no more tasks to be scheduled.
                    break;
                }
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    populateTask, blockIdx, startOffset, this, currBatch->columns(), filePath));
                startOffset += currBatch->num_rows();
                ++blockIdx;
            }
            if (currBatch == nullptr) {
                // No more batches left, thus, no more tasks to be scheduled.
                break;
            }
            taskScheduler.waitUntilEnoughTasksFinish(
                CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
        }
        taskScheduler.waitAllTasksToCompleteOrError();
    }
}

void RelCopyExecutor::populateFromParquet(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::ChunkedArray>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::ChunkedArray>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    for (auto& filePath : copyDescription.filePaths) {
        auto reader = createParquetReader(filePath);
        std::shared_ptr<arrow::Table> currTable;
        int blockIdx = 0;
        offset_t startOffset = 0;
        auto numBlocks = fileBlockInfos.at(filePath).numBlocks;
        while (blockIdx < numBlocks) {
            for (int i = 0; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
                if (blockIdx == numBlocks) {
                    break;
                }
                throwCopyExceptionIfNotOK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    populateTask, blockIdx, startOffset, this, currTable->columns(), filePath));
                startOffset += currTable->num_rows();
                ++blockIdx;
            }
            taskScheduler.waitUntilEnoughTasksFinish(
                CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
        }

        taskScheduler.waitAllTasksToCompleteOrError();
    }
}

void RelCopyExecutor::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info(
        "Populating adj columns and rel property columns for rel {}.", tableSchema->tableName);
    executePopulateTask(PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask);
    logger->info(
        "Done populating adj columns and rel property columns for rel {}.", tableSchema->tableName);
}

void RelCopyExecutor::populateLists() {
    logger->debug("Populating adjLists and rel property lists for rel {}.", tableSchema->tableName);
    executePopulateTask(PopulateTaskType::populateListsTask);
    logger->debug(
        "Done populating adjLists and rel property lists for rel {}.", tableSchema->tableName);
}

void RelCopyExecutor::sortAndCopyOverflowValues() {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        // Sort overflow values of property Lists.
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : tableSchema->properties) {
                if (property.dataType.getLogicalTypeID() == LogicalTypeID::STRING ||
                    property.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
                    offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = std::min(offsetStart + 256, numNodes);
                        auto propertyList =
                            propertyListsPerDirection[relDirection][property.propertyID].get();
                        taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                            sortOverflowValuesOfPropertyListsTask, property.dataType, offsetStart,
                            offsetEnd, adjListsPerDirection[relDirection].get(), propertyList,
                            overflowFilePerPropertyID.at(property.propertyID).get(),
                            propertyList->getInMemOverflowFile()));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    // Sort overflow values of property columns.
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (reinterpret_cast<RelTableSchema*>(tableSchema)
                ->isSingleMultiplicityInDirection(relDirection)) {
            auto numNodes =
                maxNodeOffsetsPerTable.at(
                    reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection)) +
                1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            // TODO(Semih): Schedule one at a time.
            for (auto& property : tableSchema->properties) {
                if (property.dataType.getLogicalTypeID() == LogicalTypeID::STRING ||
                    property.dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
                    offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = std::min(offsetStart + 256, numNodes);
                        auto propertyColumn =
                            propertyColumnsPerDirection[relDirection][property.propertyID].get();
                        auto propertyColumnChunk =
                            propertyColumnChunksPerDirection[relDirection][property.propertyID]
                                .get();
                        taskScheduler.scheduleTask(
                            CopyTaskFactory::createCopyTask(sortOverflowValuesOfPropertyColumnTask,
                                property.dataType, offsetStart, offsetEnd, propertyColumnChunk,
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

template<typename T>
void RelCopyExecutor::inferTableIDsAndOffsets(const std::vector<std::shared_ptr<T>>& batchColumns,
    std::vector<nodeID_t>& nodeIDs, std::vector<LogicalType>& nodeIDTypes,
    const std::map<table_id_t, PrimaryKeyIndex*>& pkIndexes, Transaction* transaction,
    int64_t blockOffset, int64_t& colIndex) {
    for (auto& relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto keyToken = batchColumns[colIndex]->GetScalar(blockOffset)->get()->ToString();
        auto keyStr = keyToken.c_str();
        ++colIndex;
        switch (nodeIDTypes[relDirection].getLogicalTypeID()) {
        case LogicalTypeID::SERIAL: {
            auto key = TypeUtils::convertStringToNumber<int64_t>(keyStr);
            nodeIDs[relDirection].offset = key;
        } break;
        case LogicalTypeID::INT64: {
            auto key = TypeUtils::convertStringToNumber<int64_t>(keyStr);
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, key, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + std::to_string(key) + " in the pkIndex.");
            }
        } break;
        case LogicalTypeID::STRING: {
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + std::string(keyStr) + " in the pkIndex.");
            }
        } break;
        default:
            throw CopyException("Unsupported data type " +
                                LogicalTypeUtils::dataTypeToString(nodeIDTypes[relDirection]) +
                                " for index lookup.");
        }
    }
}

template<typename T>
void RelCopyExecutor::putPropsOfLineIntoColumns(RelCopyExecutor* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    int64_t blockOffset, int64_t& colIndex) {
    auto& properties = copier->tableSchema->properties;
    auto& directionTablePropertyColumnChunks = copier->propertyColumnChunksPerDirection;
    auto& inMemOverflowFilePerPropertyID = copier->overflowFilePerPropertyID;
    for (auto propertyID = RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID + 1;
         propertyID < properties.size(); propertyID++) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto currentToken = batchColumns[colIndex]->GetScalar(blockOffset);
        if (!(*currentToken)->is_valid) {
            ++colIndex;
            continue;
        }
        auto stringToken =
            currentToken->get()->ToString().substr(0, BufferPoolConstants::PAGE_4KB_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyID].dataType.getLogicalTypeID()) {
        case LogicalTypeID::INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INT32: {
            auto val = TypeUtils::convertStringToNumber<int32_t>(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INT16: {
            auto val = TypeUtils::convertStringToNumber<int16_t>(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::BOOL: {
            auto val = TypeUtils::convertToBoolean(data);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::DATE: {
            auto val = Date::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::TIMESTAMP: {
            auto val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INTERVAL: {
            auto val = Interval::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::STRING: {
            auto kuStr = inMemOverflowFilePerPropertyID[propertyID]->copyString(
                data, strlen(data), inMemOverflowFileCursors[propertyID]);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case LogicalTypeID::VAR_LIST: {
            auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                properties[propertyID].dataType, copier->copyDescription);
            auto kuList = inMemOverflowFilePerPropertyID[propertyID]->copyList(
                *varListVal, inMemOverflowFileCursors[propertyID]);
            putValueIntoColumns(propertyID, directionTablePropertyColumnChunks, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuList));
        } break;
        case LogicalTypeID::FIXED_LIST: {
            auto fixedListVal = getArrowFixedList(stringToken, 1, stringToken.length() - 2,
                properties[propertyID].dataType, copier->copyDescription);
            putValueIntoColumns(
                propertyID, directionTablePropertyColumnChunks, nodeIDs, fixedListVal.get());
        } break;
        default:
            throw NotImplementedException("Not supported data type " +
                                          LogicalTypeUtils::dataTypeToString(
                                              properties[propertyID].dataType.getLogicalTypeID()) +
                                          " for RelCopyExecutor::putPropsOfLineIntoColumns.");
        }
        ++colIndex;
    }
}

template<typename T>
void RelCopyExecutor::putPropsOfLineIntoLists(RelCopyExecutor* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    const std::vector<uint64_t>& reversePos, int64_t blockOffset, int64_t& colIndex,
    CopyDescription& copyDescription) {
    auto& properties = copier->tableSchema->properties;
    auto& directionTablePropertyLists = copier->propertyListsPerDirection;
    auto& directionTableAdjLists = copier->adjListsPerDirection;
    auto& inMemOverflowFilesPerProperty = copier->overflowFilePerPropertyID;
    for (auto propertyID = RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID + 1;
         propertyID < properties.size(); propertyID++) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto currentToken = batchColumns[colIndex]->GetScalar(blockOffset);
        if (!(*currentToken)->is_valid) {
            ++colIndex;
            continue;
        }
        auto stringToken =
            currentToken->get()->ToString().substr(0, BufferPoolConstants::PAGE_4KB_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyID].dataType.getLogicalTypeID()) {
        case LogicalTypeID::INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INT32: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INT16: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::BOOL: {
            auto val = TypeUtils::convertToBoolean(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::DATE: {
            auto val = Date::FromCString(data, stringToken.length());
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::TIMESTAMP: {
            auto val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::INTERVAL: {
            auto val = Interval::FromCString(data, stringToken.length());
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case LogicalTypeID::STRING: {
            auto kuStr = inMemOverflowFilesPerProperty[propertyID]->copyString(
                data, strlen(data), inMemOverflowFileCursors[propertyID]);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case LogicalTypeID::VAR_LIST: {
            auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                properties[propertyID].dataType, copyDescription);
            auto kuList = inMemOverflowFilesPerProperty[propertyID]->copyList(
                *varListVal, inMemOverflowFileCursors[propertyID]);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuList));
        } break;
        case LogicalTypeID::FIXED_LIST: {
            auto fixedListVal = getArrowFixedList(stringToken, 1, stringToken.length() - 2,
                properties[propertyID].dataType, copyDescription);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, fixedListVal.get());
        } break;
        case LogicalTypeID::FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(data);
            putValueIntoLists(propertyID, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        default:
            throw NotImplementedException("Not supported data type " +
                                          LogicalTypeUtils::dataTypeToString(
                                              properties[propertyID].dataType.getLogicalTypeID()) +
                                          " for RelCopyExecutor::putPropsOfLineIntoLists.");
        }
        ++colIndex;
    }
}

void RelCopyExecutor::copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
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

void RelCopyExecutor::copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
    const LogicalType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        kuList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflowFromFile(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, kuList, VarListType::getChildType(&dataType));
}

template<typename T>
void RelCopyExecutor::populateAdjColumnsAndCountRelsInAdjListsTask(uint64_t blockIdx,
    uint64_t blockStartRelID, RelCopyExecutor* copier,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::string& filePath) {
    copier->logger->debug("Start: path=`{0}` blkIdx={1}", filePath, blockIdx);
    std::vector<bool> requireToReadTableLabels{true, true};
    std::vector<nodeID_t> nodeIDs{2};
    std::vector<LogicalType> nodePKTypes{2};
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(copier->tableSchema);
    for (auto& relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        auto boundTableID = relTableSchema->getBoundTableID(relDirection);
        nodeIDs[relDirection].tableID = boundTableID;
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(boundTableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    std::vector<PageByteCursor> inMemOverflowFileCursors{relTableSchema->getNumProperties()};
    uint64_t relID = blockStartRelID;
    auto numLinesInCurBlock = copier->fileBlockInfos.at(filePath).numLinesPerBlock[blockIdx];
    for (auto blockOffset = 0u; blockOffset < numLinesInCurBlock; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), blockOffset, colIndex);
        for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
            auto tableID = nodeIDs[relDirection].tableID;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
                if (!copier->adjColumnChunksPerDirection[relDirection]->isNull(nodeOffset)) {
                    throw CopyException(StringUtils::string_format(
                        "RelTable {} is a {} table, but node(nodeOffset: {}, tableName: {}) has "
                        "more than one neighbour in the {} direction.",
                        relTableSchema->tableName,
                        getRelMultiplicityAsString(relTableSchema->relMultiplicity), nodeOffset,
                        copier->catalog.getReadOnlyVersion()->getTableName(tableID),
                        RelDataDirectionUtils::relDataDirectionToString(relDirection)));
                }
                copier->adjColumnChunksPerDirection[relDirection]->setValueAtPos(
                    (uint8_t*)&nodeIDs[!relDirection].offset, nodeOffset);
            } else {
                InMemListsUtils::incrementListSize(
                    *copier->listSizesPerDirection[relDirection], nodeOffset, 1);
            }
            copier->numRels++;
        }
        if (relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoColumns<T>(
                copier, inMemOverflowFileCursors, batchColumns, nodeIDs, blockOffset, colIndex);
        }
        putValueIntoColumns(relTableSchema->getRelIDDefinition().propertyID,
            copier->propertyColumnChunksPerDirection, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", filePath, blockIdx);
}

template<typename T>
void RelCopyExecutor::populateListsTask(uint64_t blockId, uint64_t blockStartRelID,
    RelCopyExecutor* copier, const std::vector<std::shared_ptr<T>>& batchColumns,
    const std::string& filePath) {
    copier->logger->trace("Start: path=`{0}` blkIdx={1}", filePath, blockId);
    std::vector<nodeID_t> nodeIDs(2);
    std::vector<LogicalType> nodePKTypes(2);
    std::vector<uint64_t> reversePos(2);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(copier->tableSchema);
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        auto boundTableID = relTableSchema->getBoundTableID(relDirection);
        nodeIDs[relDirection].tableID = boundTableID;
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(nodeIDs[relDirection].tableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    std::vector<PageByteCursor> inMemOverflowFileCursors(relTableSchema->getNumProperties());
    uint64_t relID = blockStartRelID;
    auto numLinesInCurBlock = copier->fileBlockInfos.at(filePath).numLinesPerBlock[blockId];
    for (auto blockOffset = 0u; blockOffset < numLinesInCurBlock; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), blockOffset, colIndex);
        for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
            if (!copier->catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    copier->tableSchema->tableID, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto adjLists = copier->adjListsPerDirection[relDirection].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *copier->listSizesPerDirection[relDirection], nodeIDs[relDirection].offset, 1);
                adjLists->setElement(
                    nodeOffset, reversePos[relDirection], (uint8_t*)(&nodeIDs[!relDirection]));
            }
        }
        if (relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoLists<T>(copier, inMemOverflowFileCursors, batchColumns, nodeIDs,
                reversePos, blockOffset, colIndex, copier->copyDescription);
        }
        putValueIntoLists(relTableSchema->getRelIDDefinition().propertyID,
            copier->propertyListsPerDirection, copier->adjListsPerDirection, nodeIDs, reversePos,
            (uint8_t*)&relID);
        relID++;
    }
    copier->logger->trace("End: path=`{0}` blkIdx={1}", filePath, blockId);
}

void RelCopyExecutor::sortOverflowValuesOfPropertyColumnTask(const LogicalType& dataType,
    offset_t offsetStart, offset_t offsetEnd, InMemColumnChunk* propertyColumnChunk,
    InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        if (dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
            auto kuStr = propertyColumnChunk->getValue<ku_string_t>(offsetStart);
            copyStringOverflowFromUnorderedToOrderedPages(&kuStr, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else if (dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
            auto kuList = propertyColumnChunk->getValue<ku_list_t>(offsetStart);
            copyListOverflowFromUnorderedToOrderedPages(&kuList, dataType, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else {
            assert(false);
        }
    }
}

void RelCopyExecutor::sortOverflowValuesOfPropertyListsTask(const LogicalType& dataType,
    offset_t offsetStart, offset_t offsetEnd, InMemAdjLists* adjLists, InMemLists* propertyLists,
    InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    PageElementCursor propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        csr_offset_t listsLen = adjLists->getListSize(offsetStart);
        for (auto pos = listsLen; pos > 0; pos--) {
            propertyListCursor = propertyLists->calcPageElementCursor(pos,
                storage::StorageUtils::getDataTypeSize(dataType), offsetStart,
                true /*hasNULLBytes*/);
            if (dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
                auto kuStr = reinterpret_cast<ku_string_t*>(propertyLists->getMemPtrToLoc(
                    propertyListCursor.pageIdx, propertyListCursor.elemPosInPage));
                copyStringOverflowFromUnorderedToOrderedPages(kuStr, unorderedOverflowCursor,
                    orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
            } else if (dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
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

void RelCopyExecutor::putValueIntoColumns(uint64_t propertyID,
    std::vector<std::unordered_map<property_id_t, std::unique_ptr<InMemColumnChunk>>>&
        directionTablePropertyColumnChunks,
    const std::vector<common::nodeID_t>& nodeIDs, uint8_t* val) {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (directionTablePropertyColumnChunks[relDirection].empty()) {
            continue;
        }
        auto propertyColumnChunk =
            directionTablePropertyColumnChunks[relDirection][propertyID].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        propertyColumnChunk->setValueAtPos(val, nodeOffset);
    }
}

void RelCopyExecutor::putValueIntoLists(uint64_t propertyID,
    std::vector<std::unordered_map<common::property_id_t, std::unique_ptr<InMemLists>>>&
        directionTablePropertyLists,
    std::vector<std::unique_ptr<InMemAdjLists>>& directionTableAdjLists,
    const std::vector<nodeID_t>& nodeIDs, const std::vector<uint64_t>& reversePos, uint8_t* val) {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
        if (directionTableAdjLists[relDirection] == nullptr) {
            continue;
        }
        auto propertyList = directionTablePropertyLists[relDirection][propertyID].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        propertyList->setElement(nodeOffset, reversePos[relDirection], val);
    }
}

// Lists headers are created for only AdjLists, which store data in the page without NULL bits.
void RelCopyExecutor::calculateListHeadersTask(offset_t numNodes, atomic_uint64_vec_t* listSizes,
    ListHeadersBuilder* listHeadersBuilder, const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: ListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        offset_t chunkNodeOffset = nodeOffset;
        auto numNodesInChunk =
            std::min((offset_t)ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes - nodeOffset);
        csr_offset_t csrOffset = (*listSizes)[chunkNodeOffset].load(std::memory_order_relaxed);
        for (auto i = 1u; i < numNodesInChunk; i++) {
            auto currNodeOffset = chunkNodeOffset + i;
            auto numElementsInList = (*listSizes)[currNodeOffset].load(std::memory_order_relaxed);
            listHeadersBuilder->setCSROffset(currNodeOffset, csrOffset);
            csrOffset += numElementsInList;
        }
        listHeadersBuilder->setCSROffset(chunkNodeOffset + numNodesInChunk, csrOffset);
        nodeOffset += numNodesInChunk;
    }
    logger->trace("End: adjListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
}

void RelCopyExecutor::calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    InMemLists* inMemList, bool hasNULLBytes, const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: listsMetadataBuilder={0:p} adjListHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0;
    auto numPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(std::memory_order_relaxed);
            while (numElementsInList + offsetInPage > numPerPage) {
                numElementsInList -= (numPerPage - offsetInPage);
                numPages++;
                offsetInPage = 0;
            }
            offsetInPage += numElementsInList;
            nodeOffset++;
        }
        if (0 != offsetInPage) {
            numPages++;
        }
        inMemList->getListsMetadataBuilder()->populateChunkPageList(chunkId, numPages,
            inMemList->inMemFile->getNumPages() /* start idx of pages in .lists file */);
        inMemList->inMemFile->addNewPages(numPages);
    }
    logger->trace("End: listsMetadata={0:p} listHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
}

} // namespace storage
} // namespace kuzu
