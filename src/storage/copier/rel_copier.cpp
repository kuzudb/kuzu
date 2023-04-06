#include "storage/copier/rel_copier.h"

#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "storage/copier/copy_task.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelCopier::RelCopier(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, storage::NodesStore& nodesStore,
    BufferManager* bufferManager, table_id_t tableID, RelsStatistics* relsStatistics)
    : TableCopier{copyDescription, std::move(outputDirectory), taskScheduler, catalog, tableID,
          relsStatistics},
      nodesStore{nodesStore},
      maxNodeOffsetsPerTable{
          nodesStore.getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable()} {
    dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    initializePkIndexes(relTableSchema->srcTableID, *bufferManager);
    initializePkIndexes(relTableSchema->dstTableID, *bufferManager);
}

std::string RelCopier::getTaskTypeName(PopulateTaskType populateTaskType) {
    switch (populateTaskType) {
    case PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask: {
        return "populateAdjColumnsAndCountRelsInAdjListsTask";
    }
    case PopulateTaskType::populateListsTask: {
        return "populateListsTask";
    }
    }
}

void RelCopier::initializeColumnsAndLists() {
    for (auto relDirection : REL_DIRECTIONS) {
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
        if (property.dataType.typeID == VAR_LIST || property.dataType.typeID == STRING) {
            overflowFilePerPropertyID[property.propertyID] = std::make_unique<InMemOverflowFile>();
        }
    }
}

void RelCopier::populateColumnsAndLists() {
    populateAdjColumnsAndCountRelsInAdjLists();
    if (adjListsPerDirection[FWD] != nullptr || adjListsPerDirection[BWD] != nullptr) {
        initAdjListsHeaders();
        initListsMetadata();
        populateLists();
    }
    sortAndCopyOverflowValues();
}

void RelCopier::saveToFile() {
    logger->debug("Writing columns and Lists to disk for rel {}.", tableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        if (reinterpret_cast<RelTableSchema*>(tableSchema)
                ->isSingleMultiplicityInDirection(relDirection)) {
            adjColumnsPerDirection[relDirection]->saveToFile();
            for (auto& [_, propertyColumn] : propertyColumnsPerDirection[relDirection]) {
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

void RelCopier::initializeColumns(RelDirection relDirection) {
    auto boundTableID =
        reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
    adjColumnsPerDirection[relDirection] = std::make_unique<InMemAdjColumn>(
        StorageUtils::getAdjColumnFName(
            outputDirectory, tableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        numNodes);
    std::unordered_map<property_id_t, std::unique_ptr<InMemColumn>> propertyColumns;
    for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
        auto propertyID = tableSchema->properties[i].propertyID;
        auto propertyDataType = tableSchema->properties[i].dataType;
        auto fName = StorageUtils::getRelPropertyColumnFName(outputDirectory, tableSchema->tableID,
            relDirection, propertyID, DBFileType::WAL_VERSION);
        propertyColumns.emplace(propertyID,
            InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes));
    }
    propertyColumnsPerDirection[relDirection] = std::move(propertyColumns);
}

void RelCopier::initializeLists(RelDirection relDirection) {
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
        propertyLists.emplace(propertyID,
            InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes));
    }
    propertyListsPerDirection[relDirection] = std::move(propertyLists);
}

void RelCopier::initAdjListsHeaders() {
    logger->debug("Initializing AdjListHeaders for rel {}.", tableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(calculateListHeadersTask,
                maxNodeOffsetsPerTable.at(boundTableID) + 1, sizeof(offset_t),
                listSizesPerDirection[relDirection].get(),
                adjListsPerDirection[relDirection]->getListHeadersBuilder(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", tableSchema->tableName);
}

void RelCopier::initListsMetadata() {
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", tableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            auto adjLists = adjListsPerDirection[relDirection].get();
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto listSizes = listSizesPerDirection[relDirection].get();
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(calculateListsMetadataAndAllocateInMemListPagesTask,
                    numNodes, sizeof(offset_t), listSizes, adjLists->getListHeadersBuilder(),
                    adjLists, false /*hasNULLBytes*/, logger));
            for (auto& property : tableSchema->properties) {
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                    Types::getDataTypeSize(property.dataType), listSizes,
                    adjLists->getListHeadersBuilder(),
                    propertyListsPerDirection[relDirection][property.propertyID].get(),
                    true /*hasNULLBytes*/, logger));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing adjLists and propertyLists metadata for rel {}.",
        tableSchema->tableName);
}

void RelCopier::initializePkIndexes(table_id_t nodeTableID, BufferManager& bufferManager) {
    pkIndexes.emplace(nodeTableID, nodesStore.getPKIndex(nodeTableID));
}

arrow::Status RelCopier::executePopulateTask(PopulateTaskType populateTaskType) {
    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        status = populateFromCSV(populateTaskType);
    } break;
    case CopyDescription::FileType::PARQUET: {
        status = populateFromParquet(populateTaskType);
    } break;
    }
    return status;
}

arrow::Status RelCopier::populateFromCSV(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::Array>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::Array>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    for (auto& filePath : copyDescription.filePaths) {
        offset_t startOffset = fileBlockInfos.at(filePath).startOffset;
        std::shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        auto status = initCSVReaderAndCheckStatus(csv_streaming_reader, filePath);
        std::shared_ptr<arrow::RecordBatch> currBatch;
        int blockIdx = 0;
        auto it = csv_streaming_reader->begin();
        auto endIt = csv_streaming_reader->end();
        while (it != endIt) {
            for (int i = 0; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
                if (it == endIt) {
                    break;
                }
                ARROW_ASSIGN_OR_RAISE(currBatch, *it);
                taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                    populateTask, blockIdx, startOffset, this, currBatch->columns(), filePath));
                startOffset += currBatch->num_rows();
                ++blockIdx;
                ++it;
            }
            taskScheduler.waitUntilEnoughTasksFinish(
                CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
        }
        taskScheduler.waitAllTasksToCompleteOrError();
    }
    return arrow::Status::OK();
}

arrow::Status RelCopier::populateFromParquet(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::ChunkedArray>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::ChunkedArray>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    for (auto& filePath : copyDescription.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        auto status = initParquetReaderAndCheckStatus(reader, filePath);
        std::shared_ptr<arrow::Table> currTable;
        int blockIdx = 0;
        offset_t startOffset = 0;
        auto numBlocks = fileBlockInfos.at(filePath).numBlocks;
        while (blockIdx < numBlocks) {
            for (int i = 0; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
                if (blockIdx == numBlocks) {
                    break;
                }
                ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
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
    return arrow::Status::OK();
}

void RelCopier::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info(
        "Populating adj columns and rel property columns for rel {}.", tableSchema->tableName);
    auto status =
        executePopulateTask(PopulateTaskType::populateAdjColumnsAndCountRelsInAdjListsTask);
    throwCopyExceptionIfNotOK(status);
    logger->info(
        "Done populating adj columns and rel property columns for rel {}.", tableSchema->tableName);
}

void RelCopier::populateLists() {
    logger->debug("Populating adjLists and rel property lists for rel {}.", tableSchema->tableName);
    auto status = executePopulateTask(PopulateTaskType::populateListsTask);
    throwCopyExceptionIfNotOK(status);
    logger->debug(
        "Done populating adjLists and rel property lists for rel {}.", tableSchema->tableName);
}

void RelCopier::sortAndCopyOverflowValues() {
    for (auto relDirection : REL_DIRECTIONS) {
        // Sort overflow values of property Lists.
        if (!reinterpret_cast<RelTableSchema*>(tableSchema)
                 ->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID =
                reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection);
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : tableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == VAR_LIST) {
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
    for (auto relDirection : REL_DIRECTIONS) {
        if (reinterpret_cast<RelTableSchema*>(tableSchema)
                ->isSingleMultiplicityInDirection(relDirection)) {
            auto numNodes =
                maxNodeOffsetsPerTable.at(
                    reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(relDirection)) +
                1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : tableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == VAR_LIST) {
                    offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = std::min(offsetStart + 256, numNodes);
                        auto propertyColumn =
                            propertyColumnsPerDirection[relDirection][property.propertyID].get();
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

template<typename T>
void RelCopier::inferTableIDsAndOffsets(const std::vector<std::shared_ptr<T>>& batchColumns,
    std::vector<nodeID_t>& nodeIDs, std::vector<DataType>& nodeIDTypes,
    const std::map<table_id_t, PrimaryKeyIndex*>& pkIndexes, Transaction* transaction,
    int64_t blockOffset, int64_t& colIndex) {
    for (auto& relDirection : REL_DIRECTIONS) {
        if (colIndex >= batchColumns.size()) {
            throw CopyException("Number of columns mismatch.");
        }
        auto keyToken = batchColumns[colIndex]->GetScalar(blockOffset)->get()->ToString();
        auto keyStr = keyToken.c_str();
        ++colIndex;
        switch (nodeIDTypes[relDirection].typeID) {
        case INT64: {
            auto key = TypeUtils::convertStringToNumber<int64_t>(keyStr);
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, key, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + std::to_string(key) + " in the pkIndex.");
            }
        } break;
        case STRING: {
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw CopyException("Cannot find key: " + std::string(keyStr) + " in the pkIndex.");
            }
        } break;
        default:
            throw CopyException("Unsupported data type " +
                                Types::dataTypeToString(nodeIDTypes[relDirection]) +
                                " for index lookup.");
        }
    }
}

template<typename T>
void RelCopier::putPropsOfLineIntoColumns(RelCopier* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    int64_t blockOffset, int64_t& colIndex) {
    auto& properties = copier->tableSchema->properties;
    auto& directionTablePropertyColumns = copier->propertyColumnsPerDirection;
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
        auto stringToken =
            currentToken->get()->ToString().substr(0, BufferPoolConstants::PAGE_4KB_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case INT32: {
            auto val = TypeUtils::convertStringToNumber<int32_t>(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case INT16: {
            auto val = TypeUtils::convertStringToNumber<int16_t>(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case BOOL: {
            auto val = TypeUtils::convertToBoolean(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case DATE: {
            auto val = Date::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case TIMESTAMP: {
            auto val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case INTERVAL: {
            auto val = Interval::FromCString(data, stringToken.length());
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        case STRING: {
            auto kuStr = inMemOverflowFilePerPropertyID[propertyIdx]->copyString(
                data, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case VAR_LIST: {
            auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copier->copyDescription);
            auto kuList = inMemOverflowFilePerPropertyID[propertyIdx]->copyList(
                *varListVal, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&kuList));
        } break;
        case FIXED_LIST: {
            auto fixedListVal = getArrowFixedList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copier->copyDescription);
            putValueIntoColumns(
                propertyIdx, directionTablePropertyColumns, nodeIDs, fixedListVal.get());
        } break;
        case FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(data);
            putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                reinterpret_cast<uint8_t*>(&val));
        } break;
        default:
            break;
        }
        ++colIndex;
    }
}

template<typename T>
void RelCopier::putPropsOfLineIntoLists(RelCopier* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    const std::vector<uint64_t>& reversePos, int64_t blockOffset, int64_t& colIndex,
    CopyDescription& copyDescription) {
    auto& properties = copier->tableSchema->properties;
    auto& directionTablePropertyLists = copier->propertyListsPerDirection;
    auto& directionTableAdjLists = copier->adjListsPerDirection;
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
        auto stringToken =
            currentToken->get()->ToString().substr(0, BufferPoolConstants::PAGE_4KB_SIZE);
        const char* data = stringToken.c_str();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case INT32: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case INT16: {
            auto val = TypeUtils::convertStringToNumber<int64_t>(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case DOUBLE: {
            auto val = TypeUtils::convertStringToNumber<double_t>(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case BOOL: {
            auto val = TypeUtils::convertToBoolean(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case DATE: {
            auto val = Date::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case TIMESTAMP: {
            auto val = Timestamp::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case INTERVAL: {
            auto val = Interval::FromCString(data, stringToken.length());
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        case STRING: {
            auto kuStr = inMemOverflowFilesPerProperty[propertyIdx]->copyString(
                data, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuStr));
        } break;
        case VAR_LIST: {
            auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copyDescription);
            auto kuList = inMemOverflowFilesPerProperty[propertyIdx]->copyList(
                *varListVal, inMemOverflowFileCursors[propertyIdx]);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuList));
        } break;
        case FIXED_LIST: {
            auto fixedListVal = getArrowFixedList(stringToken, 1, stringToken.length() - 2,
                properties[propertyIdx].dataType, copyDescription);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, fixedListVal.get());
        } break;
        case FLOAT: {
            auto val = TypeUtils::convertStringToNumber<float_t>(data);
            putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&val));
        } break;
        default:
            break;
        }
        ++colIndex;
    }
}

void RelCopier::copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
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

void RelCopier::copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
    const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        kuList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflowFromFile(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, kuList, dataType.childType.get());
}

template<typename T>
void RelCopier::populateAdjColumnsAndCountRelsInAdjListsTask(uint64_t blockIdx,
    uint64_t blockStartRelID, RelCopier* copier,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::string& filePath) {
    copier->logger->debug("Start: path=`{0}` blkIdx={1}", filePath, blockIdx);
    std::vector<bool> requireToReadTableLabels{true, true};
    std::vector<nodeID_t> nodeIDs{2};
    std::vector<DataType> nodePKTypes{2};
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(copier->tableSchema);
    for (auto& relDirection : REL_DIRECTIONS) {
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
        for (auto relDirection : REL_DIRECTIONS) {
            auto tableID = nodeIDs[relDirection].tableID;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
                if (!copier->adjColumnsPerDirection[relDirection]->isNullAtNodeOffset(nodeOffset)) {
                    throw CopyException(StringUtils::string_format(
                        "RelTable {} is a {} table, but node(nodeOffset: {}, tableName: {}) has "
                        "more than one neighbour in the {} direction.",
                        relTableSchema->tableName,
                        getRelMultiplicityAsString(relTableSchema->relMultiplicity), nodeOffset,
                        copier->catalog.getReadOnlyVersion()->getTableName(tableID),
                        getRelDirectionAsString(relDirection)));
                }
                copier->adjColumnsPerDirection[relDirection]->setElement(
                    nodeOffset, (uint8_t*)&nodeIDs[!relDirection]);
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
            copier->propertyColumnsPerDirection, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", filePath, blockIdx);
}

template<typename T>
void RelCopier::populateListsTask(uint64_t blockId, uint64_t blockStartRelID, RelCopier* copier,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::string& filePath) {
    copier->logger->trace("Start: path=`{0}` blkIdx={1}", filePath, blockId);
    std::vector<nodeID_t> nodeIDs(2);
    std::vector<DataType> nodePKTypes(2);
    std::vector<uint64_t> reversePos(2);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(copier->tableSchema);
    for (auto relDirection : REL_DIRECTIONS) {
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
        for (auto relDirection : REL_DIRECTIONS) {
            if (!copier->catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    copier->tableSchema->tableID, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto adjLists = copier->adjListsPerDirection[relDirection].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *copier->listSizesPerDirection[relDirection], nodeIDs[relDirection].offset, 1);
                adjLists->setElement(adjLists->getListHeadersBuilder()->getHeader(nodeOffset),
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

void RelCopier::sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
    offset_t offsetStart, offset_t offsetEnd, InMemColumn* propertyColumn,
    InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        if (dataType.typeID == STRING) {
            auto kuStr = reinterpret_cast<ku_string_t*>(propertyColumn->getElement(offsetStart));
            copyStringOverflowFromUnorderedToOrderedPages(kuStr, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else if (dataType.typeID == VAR_LIST) {
            auto kuList = reinterpret_cast<ku_list_t*>(propertyColumn->getElement(offsetStart));
            copyListOverflowFromUnorderedToOrderedPages(kuList, dataType, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedInMemOverflowFile, orderedInMemOverflowFile);
        } else {
            assert(false);
        }
    }
}

void RelCopier::sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
    offset_t offsetStart, offset_t offsetEnd, InMemAdjLists* adjLists, InMemLists* propertyLists,
    InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile) {
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
            } else if (dataType.typeID == VAR_LIST) {
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

void RelCopier::putValueIntoColumns(uint64_t propertyIdx,
    std::vector<std::unordered_map<property_id_t, std::unique_ptr<InMemColumn>>>&
        directionTablePropertyColumns,
    const std::vector<common::nodeID_t>& nodeIDs, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (directionTablePropertyColumns[relDirection].empty()) {
            continue;
        }
        auto propertyColumn = directionTablePropertyColumns[relDirection][propertyIdx].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        propertyColumn->setElement(nodeOffset, val);
    }
}

void RelCopier::putValueIntoLists(uint64_t propertyIdx,
    std::vector<std::unordered_map<common::property_id_t, std::unique_ptr<InMemLists>>>&
        directionTablePropertyLists,
    std::vector<std::unique_ptr<InMemAdjLists>>& directionTableAdjLists,
    const std::vector<nodeID_t>& nodeIDs, const std::vector<uint64_t>& reversePos, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (directionTableAdjLists[relDirection] == nullptr) {
            continue;
        }
        auto propertyList = directionTablePropertyLists[relDirection][propertyIdx].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        auto header =
            directionTableAdjLists[relDirection]->getListHeadersBuilder()->getHeader(nodeOffset);
        propertyList->setElement(header, nodeOffset, reversePos[relDirection], val);
    }
}

// Lists headers are created for only AdjLists, which store data in the page without NULL bits.
void RelCopier::calculateListHeadersTask(offset_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: ListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
    auto numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, false /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(std::memory_order_relaxed);
            uint32_t header;
            if (numElementsInList >= numElementsPerPage) {
                header = ListHeaders::getLargeListHeader(lAdjListsIdx++);
            } else {
                header = ListHeaders::getSmallListHeader(csrOffset, numElementsInList);
                csrOffset += numElementsInList;
            }
            listHeadersBuilder->setHeader(nodeOffset, header);
            nodeOffset++;
        }
    }
    logger->trace("End: adjListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
}

void RelCopier::calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    InMemLists* inMemList, bool hasNULLBytes, const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: listsMetadataBuilder={0:p} adjListHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0;
    auto largeListIdx = 0u;
    for (auto chunkIdx = 0u; chunkIdx < numChunks; chunkIdx++) {
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            if (ListHeaders::isALargeList(listHeadersBuilder->getHeader(nodeOffset))) {
                largeListIdx++;
            }
            nodeOffset++;
        }
    }
    inMemList->getListsMetadataBuilder()->initLargeListPageLists(largeListIdx);
    nodeOffset = 0;
    largeListIdx = 0u;
    auto numPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(std::memory_order_relaxed);
            if (ListHeaders::isALargeList(listHeadersBuilder->getHeader(nodeOffset))) {
                auto numPagesForLargeList = numElementsInList / numPerPage;
                if (0 != numElementsInList % numPerPage) {
                    numPagesForLargeList++;
                }
                inMemList->getListsMetadataBuilder()->populateLargeListPageList(largeListIdx,
                    numPagesForLargeList, numElementsInList,
                    inMemList->inMemFile->getNumPages() /* start idx of pages in .lists file */);
                inMemList->inMemFile->addNewPages(numPagesForLargeList);
                largeListIdx++;
            } else {
                while (numElementsInList + offsetInPage > numPerPage) {
                    numElementsInList -= (numPerPage - offsetInPage);
                    numPages++;
                    offsetInPage = 0;
                }
                offsetInPage += numElementsInList;
            }
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
