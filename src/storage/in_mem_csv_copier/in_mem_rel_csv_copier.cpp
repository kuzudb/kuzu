#include "storage/in_mem_csv_copier/in_mem_rel_csv_copier.h"

#include "spdlog/spdlog.h"
#include "storage/in_mem_csv_copier/copy_csv_task.h"

namespace kuzu {
namespace storage {

InMemRelCSVCopier::InMemRelCSVCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog,
    map<table_id_t, node_offset_t> maxNodeOffsetsPerNodeTable, BufferManager* bufferManager,
    table_id_t tableID, RelsStatistics* relsStatistics)
    : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
      maxNodeOffsetsPerTable{move(maxNodeOffsetsPerNodeTable)}, relsStatistics{relsStatistics} {
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

uint64_t InMemRelCSVCopier::copy() {
    logger->info(
        "Copying rel {} with table {}.", relTableSchema->tableName, relTableSchema->tableID);
    calculateNumBlocks(csvDescription.filePath, relTableSchema->tableName);
    countLinesPerBlock();
    auto numRels = calculateNumRows(csvDescription.csvReaderConfig.hasHeader);
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
    relsStatistics->setNumRelsForTable(relTableSchema->tableID, numRels);
    logger->info(
        "Done copying rel {} with table {}.", relTableSchema->tableName, relTableSchema->tableID);
    return numRels;
}

void InMemRelCSVCopier::countLinesPerBlock() {
    logger->info("Counting number of lines in each block");
    numLinesPerBlock.resize(numBlocks);
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            countNumLinesPerBlockTask, csvDescription.filePath, blockId, this));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Done counting number of lines in each block.");
}

void InMemRelCSVCopier::initializeColumnsAndLists() {
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

void InMemRelCSVCopier::initializeColumns(RelDirection relDirection) {
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
        directionTablePropertyColumns[relDirection].emplace(boundTableID, move(propertyColumns));
    }
}

void InMemRelCSVCopier::initializeLists(RelDirection relDirection) {
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
        directionTablePropertyLists[relDirection].emplace(boundTableID, move(propertyLists));
    }
}

void InMemRelCSVCopier::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info(
        "Populating adj columns and rel property columns for rel {}.", relTableSchema->tableName);
    auto blockStartOffset = 0ull;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(
            CopyCSVTaskFactory::createCopyCSVTask(populateAdjColumnsAndCountRelsInAdjListsTask,
                blockIdx, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    relsStatistics->setNumRelsPerDirectionBoundTableID(
        relTableSchema->tableID, directionNumRelsPerTable);
    logger->info("Done populating adj columns and rel property columns for rel {}.",
        relTableSchema->tableName);
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

void InMemRelCSVCopier::skipFirstRowIfNecessary(
    uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader) {
    if (0 == blockId && csvDescription.csvReaderConfig.hasHeader && reader.hasNextLine()) {
        reader.skipLine();
    }
}

void InMemRelCSVCopier::populateAdjColumnsAndCountRelsInAdjListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier) {
    copier->logger->debug("Start: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
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
    auto numPropertiesToRead = copier->relTableSchema->getNumPropertiesToReadFromCSV();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferTableIDsAndOffsets(reader, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, requireToReadTableLabels);
        for (auto relDirection : REL_DIRECTIONS) {
            auto tableID = nodeIDs[relDirection].tableID;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (copier->directionTableAdjColumns[relDirection].contains(tableID)) {
                if (!copier->directionTableAdjColumns[relDirection].at(tableID)->isNullAtNodeOffset(
                        nodeOffset)) {
                    auto relTableSchema = copier->relTableSchema;
                    throw CopyCSVException(StringUtils::string_format(
                        "RelTable %s is a %s table, but node(nodeOffset: %d, tableName: %s) has "
                        "more than one neighbour in the %s direction.",
                        relTableSchema->tableName.c_str(),
                        getRelMultiplicityAsString(relTableSchema->relMultiplicity).c_str(),
                        nodeOffset,
                        copier->catalog.getReadOnlyVersion()->getNodeTableName(tableID).c_str(),
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
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoColumns(numPropertiesToRead, copier->directionTablePropertyColumns,
                copier->relTableSchema->properties, copier->overflowFilePerPropertyID,
                inMemOverflowFileCursors, reader, nodeIDs);
        }
        putValueIntoColumns(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->directionTablePropertyColumns, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemRelCSVCopier::putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
    vector<table_property_in_mem_columns_map_t>& directionTablePropertyColumns,
    const vector<Property>& properties,
    unordered_map<uint32_t, unique_ptr<InMemOverflowFile>>& inMemOverflowFilePerPropertyID,
    vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
    const vector<nodeID_t>& nodeIDs) {
    for (auto propertyIdx = 0u; propertyIdx < numPropertiesToRead; propertyIdx++) {
        reader.hasNextTokenOrError();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto int64Val = reader.getInt64();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&int64Val));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto kuStr = inMemOverflowFilePerPropertyID[propertyIdx]->copyString(
                    strVal, inMemOverflowFileCursors[propertyIdx]);
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&kuStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                auto kuList = inMemOverflowFilePerPropertyID[propertyIdx]->copyList(
                    listVal, inMemOverflowFileCursors[propertyIdx]);
                putValueIntoColumns(propertyIdx, directionTablePropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&kuList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemRelCSVCopier::inferTableIDsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<DataType>& nodeIDTypes, const map<table_id_t, unique_ptr<PrimaryKeyIndex>>& pkIndexes,
    Transaction* transaction, const Catalog& catalog, vector<bool> requireToReadTableLabels) {
    for (auto& relDirection : REL_DIRECTIONS) {
        reader.hasNextToken();
        if (requireToReadTableLabels[relDirection]) {
            auto nodeTableName = reader.getString();
            if (!catalog.getReadOnlyVersion()->containNodeTable(nodeTableName)) {
                throw CopyCSVException(
                    "NodeTableName: " + string(nodeTableName) + " does not exist.");
            }
            nodeIDs[relDirection].tableID =
                catalog.getReadOnlyVersion()->getNodeTableIDFromName(reader.getString());
            reader.hasNextToken();
        }
        auto keyStr = reader.getString();
        switch (nodeIDTypes[relDirection].typeID) {
        case INT64: {
            auto key = TypeUtils::convertToInt64(keyStr);
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, key, nodeIDs[relDirection].offset)) {
                throw CopyCSVException("Cannot find key: " + to_string(key) + " in the pkIndex.");
            }
        } break;
        case STRING: {
            if (!pkIndexes.at(nodeIDs[relDirection].tableID)
                     ->lookup(transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw CopyCSVException("Cannot find key: " + string(keyStr) + " in the pkIndex.");
            }
        } break;
        default:
            throw CopyCSVException("Unsupported data type " +
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

void InMemRelCSVCopier::putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
    vector<table_property_in_mem_lists_map_t>& directionTablePropertyLists,
    vector<table_adj_in_mem_lists_map_t>& directionTableAdjLists,
    const vector<Property>& properties,
    unordered_map<uint32_t, unique_ptr<InMemOverflowFile>>& inMemOverflowFilesPerProperty,
    vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
    const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos) {
    for (auto propertyIdx = 0u; propertyIdx < numPropertiesToRead; propertyIdx++) {
        reader.hasNextToken();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto intVal = reader.getInt64();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&intVal));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto kuStr = inMemOverflowFilesPerProperty[propertyIdx]->copyString(
                    strVal, inMemOverflowFileCursors[propertyIdx]);
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                auto kuList = inMemOverflowFilesPerProperty[propertyIdx]->copyList(
                    listVal, inMemOverflowFileCursors[propertyIdx]);
                putValueIntoLists(propertyIdx, directionTablePropertyLists, directionTableAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&kuList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemRelCSVCopier::initAdjListsHeaders() {
    logger->debug("Initializing AdjListHeaders for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [boundTableID, adjList] : directionTableAdjLists[relDirection]) {
            auto numBytesPerNode = directionNodeIDCompressionScheme[relDirection][boundTableID]
                                       .getNumBytesForNodeIDAfterCompression();
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                calculateListHeadersTask, maxNodeOffsetsPerTable.at(boundTableID) + 1,
                numBytesPerNode, directionTableListSizes[relDirection][boundTableID].get(),
                adjList->getListHeadersBuilder(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", relTableSchema->tableName);
}

void InMemRelCSVCopier::initListsMetadata() {
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", relTableSchema->tableName);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [boundTableID, adjList] : directionTableAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) + 1;
            auto listSizes = directionTableListSizes[relDirection][boundTableID].get();
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                directionNodeIDCompressionScheme[relDirection][boundTableID]
                    .getNumBytesForNodeIDAfterCompression(),
                listSizes, adjList->getListHeadersBuilder(), adjList.get(), false /*hasNULLBytes*/,
                logger));
            for (auto& property : relTableSchema->properties) {
                taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
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

void InMemRelCSVCopier::populateLists() {
    logger->debug(
        "Populating adjLists and rel property lists for rel {}.", relTableSchema->tableName);
    auto blockStartOffset = 0ull;
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            populateListsTask, blockId, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockId];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug(
        "Done populating adjLists and rel property lists for rel {}.", relTableSchema->tableName);
}

void InMemRelCSVCopier::populateListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier) {
    copier->logger->trace("Start: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
    vector<bool> requireToReadTableLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodePKTypes{2};
    vector<uint64_t> reversePos{2};
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
    auto numPropertiesToRead = copier->relTableSchema->getNumPropertiesToReadFromCSV();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferTableIDsAndOffsets(reader, nodeIDs, nodePKTypes, copier->pkIndexes,
            copier->dummyReadOnlyTrx.get(), copier->catalog, requireToReadTableLabels);
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
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoLists(numPropertiesToRead, copier->directionTablePropertyLists,
                copier->directionTableAdjLists, copier->relTableSchema->properties,
                copier->overflowFilePerPropertyID, inMemOverflowFileCursors, reader, nodeIDs,
                reversePos);
        }
        putValueIntoLists(copier->relTableSchema->getRelIDDefinition().propertyID,
            copier->directionTablePropertyLists, copier->directionTableAdjLists, nodeIDs,
            reversePos, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->trace("End: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemRelCSVCopier::copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
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

void InMemRelCSVCopier::copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
    const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        kuList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflow(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, kuList, dataType.childType.get());
}

void InMemRelCSVCopier::sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
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

void InMemRelCSVCopier::sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
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

void InMemRelCSVCopier::sortAndCopyOverflowValues() {
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
                        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
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
                        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                            sortOverflowValuesOfPropertyColumnTask, property.dataType, offsetStart,
                            offsetEnd, propertyColumn,
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

void InMemRelCSVCopier::saveToFile() {
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

} // namespace storage
} // namespace kuzu
