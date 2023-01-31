#include "storage/copy_arrow/copy_rel_arrow.h"

#include "spdlog/spdlog.h"
#include "storage/copy_arrow/copy_task.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

CopyRelArrow::CopyRelArrow(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog,
    std::map<table_id_t, offset_t> maxNodeOffsetsPerNodeTable, BufferManager* bufferManager,
    table_id_t tableID, RelsStatistics* relsStatistics)
    : CopyStructuresArrow{copyDescription, std::move(outputDirectory), taskScheduler, catalog},
      maxNodeOffsetsPerTable{std::move(maxNodeOffsetsPerNodeTable)}, relsStatistics{
                                                                         relsStatistics} {
    dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
    initializePkIndexes(relTableSchema->srcTableID, *bufferManager);
    initializePkIndexes(relTableSchema->dstTableID, *bufferManager);
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
    if (adjListsPerDirection[FWD] != nullptr || adjListsPerDirection[BWD] != nullptr) {
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
    for (auto relDirection : REL_DIRECTIONS) {
        listSizesPerDirection[relDirection] = std::make_unique<atomic_uint64_vec_t>(
            maxNodeOffsetsPerTable.at(relTableSchema->getBoundTableID(relDirection)) + 1);
        if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relTableSchema->tableID, relDirection)) {
            initializeColumns(relDirection);
        } else {
            initializeLists(relDirection);
        }
    }
    for (auto& property : relTableSchema->properties) {
        if (property.dataType.typeID == LIST || property.dataType.typeID == STRING) {
            overflowFilePerPropertyID[property.propertyID] = std::make_unique<InMemOverflowFile>();
        }
    }
}

void CopyRelArrow::initializeColumns(RelDirection relDirection) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
    adjColumnsPerDirection[relDirection] = make_unique<InMemAdjColumn>(
        StorageUtils::getAdjColumnFName(
            outputDirectory, relTableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        numNodes);
    std::unordered_map<property_id_t, std::unique_ptr<InMemColumn>> propertyColumns;
    for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
        auto propertyID = relTableSchema->properties[i].propertyID;
        auto propertyDataType = relTableSchema->properties[i].dataType;
        auto fName = StorageUtils::getRelPropertyColumnFName(outputDirectory,
            relTableSchema->tableID, relDirection, propertyID, DBFileType::WAL_VERSION);
        propertyColumns.emplace(propertyID,
            InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes));
    }
    propertyColumnsPerDirection[relDirection] = std::move(propertyColumns);
}

void CopyRelArrow::initializeLists(RelDirection relDirection) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
    adjListsPerDirection[relDirection] = make_unique<InMemAdjLists>(
        StorageUtils::getAdjListsFName(
            outputDirectory, relTableSchema->tableID, relDirection, DBFileType::WAL_VERSION),
        numNodes);
    std::unordered_map<property_id_t, std::unique_ptr<InMemLists>> propertyLists;
    for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
        auto propertyID = relTableSchema->properties[i].propertyID;
        auto propertyDataType = relTableSchema->properties[i].dataType;
        auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory,
            relTableSchema->tableID, relDirection, propertyID, DBFileType::WAL_VERSION);
        propertyLists.emplace(propertyID,
            InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes));
    }
    propertyListsPerDirection[relDirection] = std::move(propertyLists);
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
    logger->info("Done populating adj columns and rel property columns for rel {}.",
        relTableSchema->tableName);
}

arrow::Status CopyRelArrow::populateFromCSV(PopulateTaskType populateTaskType) {
    auto populateTask = populateAdjColumnsAndCountRelsInAdjListsTask<arrow::Array>;
    if (populateTaskType == PopulateTaskType::populateListsTask) {
        populateTask = populateListsTask<arrow::Array>;
    }
    logger->debug("Assigning task {0}", getTaskTypeName(populateTaskType));

    std::shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
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
                blockStartOffset, this, currBatch->columns(), copyDescription));
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
                blockStartOffset, this, currBatch->columns(), copyDescription));
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
                blockStartOffset, this, currTable->columns(), copyDescription));
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
    std::vector<std::unordered_map<property_id_t, std::unique_ptr<InMemColumn>>>&
        directionTablePropertyColumns,
    const std::vector<common::nodeID_t>& nodeIDs, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (directionTablePropertyColumns[relDirection].size() == 0) {
            continue;
        }
        auto propertyColumn = directionTablePropertyColumns[relDirection][propertyIdx].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        propertyColumn->setElement(nodeOffset, val);
    }
}

template<typename T>
void CopyRelArrow::populateAdjColumnsAndCountRelsInAdjListsTask(uint64_t blockId,
    uint64_t blockStartRelID, CopyRelArrow* copier,
    const std::vector<std::shared_ptr<T>>& batchColumns, CopyDescription& copyDescription) {
    copier->logger->debug(
        "Start: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
    std::vector<bool> requireToReadTableLabels{true, true};
    std::vector<nodeID_t> nodeIDs{2};
    std::vector<DataType> nodePKTypes{2};
    for (auto& relDirection : REL_DIRECTIONS) {
        auto boundTableID = copier->relTableSchema->getBoundTableID(relDirection);
        nodeIDs[relDirection].tableID = boundTableID;
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(boundTableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    std::vector<PageByteCursor> inMemOverflowFileCursors{
        copier->relTableSchema->getNumProperties()};
    uint64_t relID = blockStartRelID;
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, blockOffset, colIndex);
        for (auto relDirection : REL_DIRECTIONS) {
            auto tableID = nodeIDs[relDirection].tableID;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (copier->relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
                if (!copier->adjColumnsPerDirection[relDirection]->isNullAtNodeOffset(nodeOffset)) {
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
                copier->adjColumnsPerDirection[relDirection]->setElement(
                    nodeOffset, (uint8_t*)&nodeIDs[!relDirection]);
            } else {
                InMemListsUtils::incrementListSize(
                    *copier->listSizesPerDirection[relDirection], nodeOffset, 1);
            }
            copier->numRels++;
        }
        if (copier->relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoColumns<T>(copier, inMemOverflowFileCursors, batchColumns, nodeIDs,
                blockOffset, colIndex, copyDescription);
        }
        putValueIntoColumns(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->propertyColumnsPerDirection, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
}

template<typename T>
void CopyRelArrow::putPropsOfLineIntoColumns(CopyRelArrow* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    int64_t blockOffset, int64_t& colIndex, CopyDescription& copyDescription) {
    auto& properties = copier->relTableSchema->properties;
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
void CopyRelArrow::inferTableIDsAndOffsets(const std::vector<std::shared_ptr<T>>& batchColumns,
    std::vector<nodeID_t>& nodeIDs, std::vector<DataType>& nodeIDTypes,
    const std::map<table_id_t, std::unique_ptr<PrimaryKeyIndex>>& pkIndexes,
    Transaction* transaction, const Catalog& catalog, int64_t blockOffset, int64_t& colIndex) {
    for (auto& relDirection : REL_DIRECTIONS) {
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

static void putValueIntoLists(uint64_t propertyIdx,
    std::vector<std::unordered_map<common::property_id_t, std::unique_ptr<InMemLists>>>&
        directionTablePropertyLists,
    std::vector<std::unique_ptr<InMemAdjLists>>& directionTableAdjLists,
    const std::vector<nodeID_t>& nodeIDs, const std::vector<uint64_t>& reversePos, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (directionTableAdjLists[relDirection] == nullptr) {
            continue;
        }
        auto tableID = nodeIDs[relDirection].tableID;
        auto propertyList = directionTablePropertyLists[relDirection][propertyIdx].get();
        auto nodeOffset = nodeIDs[relDirection].offset;
        auto header =
            directionTableAdjLists[relDirection]->getListHeadersBuilder()->getHeader(nodeOffset);
        propertyList->setElement(header, nodeOffset, reversePos[relDirection], val);
    }
}

template<typename T>
void CopyRelArrow::putPropsOfLineIntoLists(CopyRelArrow* copier,
    std::vector<PageByteCursor>& inMemOverflowFileCursors,
    const std::vector<std::shared_ptr<T>>& batchColumns, const std::vector<nodeID_t>& nodeIDs,
    const std::vector<uint64_t>& reversePos, int64_t blockOffset, int64_t& colIndex,
    CopyDescription& copyDescription) {
    auto& properties = copier->relTableSchema->properties;
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
        if (!relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID = relTableSchema->getBoundTableID(relDirection);
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(calculateListHeadersTask,
                maxNodeOffsetsPerTable.at(boundTableID) + 1, sizeof(offset_t),
                listSizesPerDirection[relDirection].get(),
                adjListsPerDirection[relDirection]->getListHeadersBuilder(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", relTableSchema->tableName);
}

void CopyRelArrow::initListsMetadata() {
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        if (!relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID = relTableSchema->getBoundTableID(relDirection);
            auto adjLists = adjListsPerDirection[relDirection].get();
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto listSizes = listSizesPerDirection[relDirection].get();
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(calculateListsMetadataAndAllocateInMemListPagesTask,
                    numNodes, sizeof(offset_t), listSizes, adjLists->getListHeadersBuilder(),
                    adjLists, false /*hasNULLBytes*/, logger));
            for (auto& property : relTableSchema->properties) {
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
        relTableSchema->tableName);
}

void CopyRelArrow::initializePkIndexes(table_id_t nodeTableID, BufferManager& bufferManager) {
    pkIndexes.emplace(nodeTableID,
        std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(this->outputDirectory, nodeTableID),
            catalog.getReadOnlyVersion()->getNodeTableSchema(nodeTableID)->getPrimaryKey().dataType,
            bufferManager, nullptr /* wal */));
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
    CopyRelArrow* copier, const std::vector<std::shared_ptr<T>>& batchColumns,
    CopyDescription& copyDescription) {
    copier->logger->trace(
        "Start: path=`{0}` blkIdx={1}", copier->copyDescription.filePath, blockId);
    std::vector<nodeID_t> nodeIDs(2);
    std::vector<DataType> nodePKTypes(2);
    std::vector<uint64_t> reversePos(2);
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableID = copier->relTableSchema->getBoundTableID(relDirection);
        nodeIDs[relDirection].tableID = boundTableID;
        nodePKTypes[relDirection] = copier->catalog.getReadOnlyVersion()
                                        ->getNodeTableSchema(nodeIDs[relDirection].tableID)
                                        ->getPrimaryKey()
                                        .dataType;
    }
    std::vector<PageByteCursor> inMemOverflowFileCursors(
        copier->relTableSchema->getNumProperties());
    uint64_t relID = blockStartRelID;
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        int64_t colIndex = 0;
        inferTableIDsAndOffsets(batchColumns, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, blockOffset, colIndex);
        for (auto relDirection : REL_DIRECTIONS) {
            if (!copier->catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    copier->relTableSchema->tableID, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto tableID = nodeIDs[relDirection].tableID;
                auto adjLists = copier->adjListsPerDirection[relDirection].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *copier->listSizesPerDirection[relDirection], nodeIDs[relDirection].offset, 1);
                adjLists->setElement(adjLists->getListHeadersBuilder()->getHeader(nodeOffset),
                    nodeOffset, reversePos[relDirection], (uint8_t*)(&nodeIDs[!relDirection]));
            }
        }
        if (copier->relTableSchema->getNumUserDefinedProperties() != 0) {
            putPropsOfLineIntoLists<T>(copier, inMemOverflowFileCursors, batchColumns, nodeIDs,
                reversePos, blockOffset, colIndex, copyDescription);
        }
        putValueIntoLists(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->propertyListsPerDirection, copier->adjListsPerDirection, nodeIDs, reversePos,
            (uint8_t*)&relID);
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
    offset_t offsetStart, offset_t offsetEnd, InMemColumn* propertyColumn,
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
        if (!relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            auto boundTableID = relTableSchema->getBoundTableID(relDirection);
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relTableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
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
        if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            auto numNodes =
                maxNodeOffsetsPerTable.at(relTableSchema->getBoundTableID(relDirection)) + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relTableSchema->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
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

void CopyRelArrow::saveToFile() {
    logger->debug("Writing columns and Lists to disk for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
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
