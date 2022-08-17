#include "include/in_mem_rel_csv_copier.h"

#include "include/copy_csv_task.h"
#include "spdlog/spdlog.h"

namespace graphflow {
namespace storage {

InMemRelCSVCopier::InMemRelCSVCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, vector<uint64_t> maxNodeOffsetsPerNodeLabel,
    BufferManager* bufferManager, label_t labelID)
    : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
      maxNodeOffsetsPerNodeLabel{move(maxNodeOffsetsPerNodeLabel)},
      startRelID{catalog.getReadOnlyVersion()->getNextRelID()},
      relLabel{catalog.getReadOnlyVersion()->getRelLabel(labelID)} {
    tmpReadTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    IDIndexes.resize(catalog.getReadOnlyVersion()->getNumNodeLabels());
    for (auto& nodeLabel : relLabel->getAllNodeLabels()) {
        assert(IDIndexes[nodeLabel] == nullptr);
        IDIndexes[nodeLabel] = make_unique<HashIndex>(
            StorageUtils::getNodeIndexIDAndFName(this->outputDirectory, nodeLabel),
            catalog.getReadOnlyVersion()->getNodeLabel(nodeLabel)->getPrimaryKey().dataType,
            *bufferManager, true /* isInMemory */);
    }
}

void InMemRelCSVCopier::copy() {
    logger->info("Copying rel {} with label {}.", relLabel->labelName, relLabel->labelID);
    calculateNumBlocks(csvDescription.filePath, relLabel->labelName);
    countLinesPerBlock();
    auto numRels = calculateNumRows();
    initializeColumnsAndLists();
    // Construct columns and lists.
    populateAdjColumnsAndCountRelsInAdjLists();
    if (!directionLabelAdjLists[FWD].empty() || !directionLabelAdjLists[BWD].empty()) {
        initAdjListsHeaders();
        initAdjAndPropertyListsMetadata();
        populateAdjAndPropertyLists();
        sortOverflowValues();
    }
    saveToFile();
    relLabel->setNumRels(numRels);
    logger->info("Done copying rel {} with label {}.", relLabel->labelName, relLabel->labelID);
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
    for (auto relDirection : REL_DIRECTIONS) {
        directionNodeIDCompressionScheme[relDirection] =
            NodeIDCompressionScheme(catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
                                        relLabel->labelID, !relDirection),
                maxNodeOffsetsPerNodeLabel);
        directionNumRelsPerLabel[relDirection] =
            make_unique<atomic_uint64_vec_t>(catalog.getReadOnlyVersion()->getNumNodeLabels());
        directionLabelListSizes[relDirection].resize(
            catalog.getReadOnlyVersion()->getNumNodeLabels());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getReadOnlyVersion()->getNumNodeLabels();
             nodeLabel++) {
            directionLabelListSizes[relDirection][nodeLabel] = make_unique<atomic_uint64_vec_t>(
                maxNodeOffsetsPerNodeLabel[nodeLabel] + 1 /* num nodes */);
        }
        if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relLabel->labelID, relDirection)) {
            initializeColumns(relDirection);
        } else {
            initializeLists(relDirection);
        }
    }
    propertyListsOverflowFiles.resize(relLabel->getNumProperties());
    propertyColumnsOverflowFiles.resize(relLabel->getNumProperties());
    for (auto& property : relLabel->properties) {
        if (property.dataType.typeID == LIST || property.dataType.typeID == STRING) {
            propertyListsOverflowFiles[property.propertyID] = make_unique<InMemOverflowFile>();
            propertyColumnsOverflowFiles[property.propertyID] = make_unique<InMemOverflowFile>();
        }
    }
}

void InMemRelCSVCopier::initializeColumns(RelDirection relDirection) {
    auto nodeLabels = catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
        relLabel->labelID, relDirection);
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
        directionLabelAdjColumns[relDirection].emplace(
            nodeLabel, make_unique<InMemAdjColumn>(StorageUtils::getAdjColumnFName(outputDirectory,
                                                       relLabel->labelID, nodeLabel, relDirection),
                           directionNodeIDCompressionScheme[relDirection], numNodes));
        vector<unique_ptr<InMemColumn>> propertyColumns(relLabel->getNumProperties());
        for (auto i = 0u; i < relLabel->getNumProperties(); ++i) {
            auto propertyName = relLabel->properties[i].name;
            auto propertyDataType = relLabel->properties[i].dataType;
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, relLabel->labelID, nodeLabel, relDirection, propertyName);
            propertyColumns[i] =
                InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes);
        }
        directionLabelPropertyColumns[relDirection].emplace(nodeLabel, move(propertyColumns));
    }
}

void InMemRelCSVCopier::initializeLists(RelDirection relDirection) {
    auto nodeLabels = catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
        relLabel->labelID, relDirection);
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
        directionLabelAdjLists[relDirection].emplace(
            nodeLabel, make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(outputDirectory,
                                                      relLabel->labelID, nodeLabel, relDirection),
                           directionNodeIDCompressionScheme[relDirection], numNodes));
        vector<unique_ptr<InMemLists>> propertyLists(relLabel->getNumProperties());
        for (auto i = 0u; i < relLabel->getNumProperties(); ++i) {
            auto propertyName = relLabel->properties[i].name;
            auto propertyDataType = relLabel->properties[i].dataType;
            auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory, relLabel->labelID,
                nodeLabel, relDirection, relLabel->properties[i].propertyID);
            propertyLists[i] =
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
        }
        directionLabelPropertyLists[relDirection].emplace(nodeLabel, move(propertyLists));
    }
}

void InMemRelCSVCopier::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info(
        "Populating adj columns and rel property columns for rel {}.", relLabel->labelName);
    auto blockStartOffset = 0ull;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(
            CopyCSVTaskFactory::createCopyCSVTask(populateAdjColumnsAndCountRelsInAdjListsTask,
                blockIdx, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    populateNumRels();
    logger->info(
        "Done populating adj columns and rel property columns for rel {}.", relLabel->labelName);
}

static void putValueIntoColumns(uint64_t propertyIdx,
    vector<label_property_in_mem_columns_map_t>& directionLabelPropertyColumns,
    const vector<nodeID_t>& nodeIDs, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabel = nodeIDs[relDirection].label;
        if (directionLabelPropertyColumns[relDirection].contains(nodeLabel)) {
            auto propertyColumn =
                directionLabelPropertyColumns[relDirection].at(nodeLabel)[propertyIdx].get();
            auto nodeOffset = nodeIDs[relDirection].offset;
            propertyColumn->setElement(nodeOffset, val);
        }
    }
}

void InMemRelCSVCopier::populateAdjColumnsAndCountRelsInAdjListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier) {
    copier->logger->debug("Start: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodeIDTypes{2};
    for (auto& relDirection : REL_DIRECTIONS) {
        auto nodeLabels = copier->catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
            copier->relLabel->labelID, relDirection);
        requireToReadLabels[relDirection] = nodeLabels.size() != 1;
        nodeIDs[relDirection].label = *nodeLabels.begin();
        nodeIDTypes[relDirection] =
            copier->catalog.getReadOnlyVersion()
                ->getNodeProperty(nodeIDs[relDirection].label, CopyCSVConfig::ID_FIELD)
                .dataType;
    }
    vector<PageByteCursor> overflowPagesCursors{copier->relLabel->getNumProperties()};
    auto numPropertiesToRead = copier->relLabel->getNumPropertiesToReadFromCSV();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDTypes, copier->IDIndexes,
            copier->tmpReadTransaction.get(), copier->catalog, requireToReadLabels);
        for (auto relDirection : REL_DIRECTIONS) {
            auto nodeLabel = nodeIDs[relDirection].label;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (copier->directionLabelAdjColumns[relDirection].contains(nodeLabel)) {
                copier->directionLabelAdjColumns[relDirection].at(nodeLabel)->setElement(
                    nodeOffset, (uint8_t*)&nodeIDs[!relDirection]);
            } else {
                InMemListsUtils::incrementListSize(
                    *copier->directionLabelListSizes[relDirection].at(nodeLabel), nodeOffset, 1);
            }
            copier->directionNumRelsPerLabel[relDirection]->operator[](nodeLabel)++;
        }
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoColumns(numPropertiesToRead, copier->directionLabelPropertyColumns,
                copier->relLabel->properties, copier->propertyColumnsOverflowFiles,
                overflowPagesCursors, reader, nodeIDs);
        }
        putValueIntoColumns(copier->relLabel->getRelIDDefinition().propertyID,
            copier->directionLabelPropertyColumns, nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->debug("End: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemRelCSVCopier::putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
    vector<label_property_in_mem_columns_map_t>& directionLabelPropertyColumns,
    const vector<Property>& properties, vector<unique_ptr<InMemOverflowFile>>& overflowPages,
    vector<PageByteCursor>& overflowCursors, CSVReader& reader, const vector<nodeID_t>& nodeIDs) {
    for (auto propertyIdx = 0u; propertyIdx < numPropertiesToRead; propertyIdx++) {
        reader.hasNextToken();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto int64Val = reader.getInt64();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&int64Val));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto gfStr =
                    overflowPages[propertyIdx]->copyString(strVal, overflowCursors[propertyIdx]);
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&gfStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                auto gfList =
                    overflowPages[propertyIdx]->copyList(listVal, overflowCursors[propertyIdx]);
                putValueIntoColumns(propertyIdx, directionLabelPropertyColumns, nodeIDs,
                    reinterpret_cast<uint8_t*>(&gfList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemRelCSVCopier::inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<DataType>& nodeIDTypes, const vector<unique_ptr<HashIndex>>& IDIndexes,
    Transaction* transaction, const Catalog& catalog, vector<bool>& requireToReadLabels) {
    for (auto& relDirection : REL_DIRECTIONS) {
        reader.hasNextToken();
        if (requireToReadLabels[relDirection]) {
            nodeIDs[relDirection].label =
                catalog.getReadOnlyVersion()->getNodeLabelFromName(reader.getString());
        } else {
            reader.skipToken();
        }
        reader.hasNextToken();
        auto keyStr = reader.getString();
        switch (nodeIDTypes[relDirection].typeID) {
        case INT64: {
            auto key = TypeUtils::convertToInt64(keyStr);
            if (!IDIndexes[nodeIDs[relDirection].label]->lookup(
                    transaction, key, nodeIDs[relDirection].offset)) {
                throw CopyCSVException("Cannot find key: " + to_string(key) + " in the IDIndex.");
            }
        } break;
        case STRING: {
            if (!IDIndexes[nodeIDs[relDirection].label]->lookup(
                    transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw CopyCSVException("Cannot find key: " + string(keyStr) + " in the IDIndex.");
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
    vector<label_property_in_mem_lists_map_t>& directionLabelPropertyLists,
    vector<label_adj_in_mem_lists_map_t>& directionLabelAdjLists, const vector<nodeID_t>& nodeIDs,
    const vector<uint64_t>& reversePos, uint8_t* val) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabel = nodeIDs[relDirection].label;
        if (directionLabelPropertyLists[relDirection].contains(nodeLabel)) {
            auto propertyList =
                directionLabelPropertyLists[relDirection].at(nodeLabel)[propertyIdx].get();
            auto nodeOffset = nodeIDs[relDirection].offset;
            auto header =
                directionLabelAdjLists[relDirection][nodeLabel]->getListHeadersBuilder()->getHeader(
                    nodeOffset);
            propertyList->setElement(header, nodeOffset, reversePos[relDirection], val);
        }
    }
}

void InMemRelCSVCopier::putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
    vector<label_property_in_mem_lists_map_t>& directionLabelPropertyLists,
    vector<label_adj_in_mem_lists_map_t>& directionLabelAdjLists,
    const vector<Property>& properties, vector<unique_ptr<InMemOverflowFile>>& overflowPages,
    vector<PageByteCursor>& overflowCursors, CSVReader& reader, const vector<nodeID_t>& nodeIDs,
    const vector<uint64_t>& reversePos) {
    for (auto propertyIdx = 0u; propertyIdx < numPropertiesToRead; propertyIdx++) {
        reader.hasNextToken();
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto intVal = reader.getInt64();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&intVal));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto gfStr =
                    overflowPages[propertyIdx]->copyString(strVal, overflowCursors[propertyIdx]);
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&gfStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                auto gfList =
                    overflowPages[propertyIdx]->copyList(listVal, overflowCursors[propertyIdx]);
                putValueIntoLists(propertyIdx, directionLabelPropertyLists, directionLabelAdjLists,
                    nodeIDs, reversePos, reinterpret_cast<uint8_t*>(&gfList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemRelCSVCopier::populateNumRels() {
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto boundNodeLabel : catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
                 relLabel->labelID, relDirection)) {
            relLabel->numRelsPerDirectionBoundLabel[relDirection][boundNodeLabel] =
                directionNumRelsPerLabel[relDirection]->operator[](boundNodeLabel).load();
        }
    }
}

void InMemRelCSVCopier::initAdjListsHeaders() {
    logger->debug("Initializing AdjListHeaders for rel {}.", relLabel->labelName);
    for (auto relDirection : REL_DIRECTIONS) {
        auto numBytesPerNode = directionNodeIDCompressionScheme[relDirection].getNumTotalBytes();
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                calculateListHeadersTask, maxNodeOffsetsPerNodeLabel[nodeLabel] + 1,
                numBytesPerNode, directionLabelListSizes[relDirection][nodeLabel].get(),
                adjList->getListHeadersBuilder(), logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing AdjListHeaders for rel {}.", relLabel->labelName);
}

uint64_t InMemRelCSVCopier::getNumTasksOfInitializingAdjAndPropertyListsMetadata() {
    auto numTasks = 0;
    for (auto relDirection : REL_DIRECTIONS) {
        numTasks += directionLabelAdjLists[relDirection].size() +
                    directionLabelAdjLists[relDirection].size() * relLabel->getNumProperties();
    }
    return numTasks;
}

void InMemRelCSVCopier::initAdjAndPropertyListsMetadata() {
    logger->debug(
        "Initializing adjLists and propertyLists metadata for rel {}.", relLabel->labelName);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
            auto listSizes = directionLabelListSizes[relDirection][nodeLabel].get();
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                directionNodeIDCompressionScheme[relDirection].getNumTotalBytes(), listSizes,
                adjList->getListHeadersBuilder(), adjList.get(), false /*hasNULLBytes*/, logger));
            for (auto& property : relLabel->properties) {
                taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                    Types::getDataTypeSize(property.dataType), listSizes,
                    adjList->getListHeadersBuilder(),
                    directionLabelPropertyLists[relDirection][nodeLabel][property.propertyID].get(),
                    true /*hasNULLBytes*/, logger));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug(
        "Done initializing adjLists and propertyLists metadata for rel {}.", relLabel->labelName);
}

void InMemRelCSVCopier::populateAdjAndPropertyLists() {
    logger->debug("Populating adjLists and rel property lists for rel {}.", relLabel->labelName);
    auto blockStartOffset = 0ull;
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            populateAdjAndPropertyListsTask, blockId, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockId];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug(
        "Done populating adjLists and rel property lists for rel {}.", relLabel->labelName);
}

void InMemRelCSVCopier::populateAdjAndPropertyListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier) {
    copier->logger->trace("Start: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodeIDTypes{2};
    vector<uint64_t> reversePos{2};
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels = copier->catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
            copier->relLabel->labelID, relDirection);
        requireToReadLabels[relDirection] = nodeLabels.size() != 1;
        nodeIDs[relDirection].label = *nodeLabels.begin();
        nodeIDTypes[relDirection] =
            copier->catalog.getReadOnlyVersion()
                ->getNodeProperty(nodeIDs[relDirection].label, CopyCSVConfig::ID_FIELD)
                .dataType;
    }
    vector<PageByteCursor> overflowPagesCursors(copier->relLabel->getNumProperties());
    auto numPropertiesToRead = copier->relLabel->getNumPropertiesToReadFromCSV();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDTypes, copier->IDIndexes,
            copier->tmpReadTransaction.get(), copier->catalog, requireToReadLabels);
        for (auto relDirection : REL_DIRECTIONS) {
            if (!copier->catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    copier->relLabel->labelID, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto nodeLabel = nodeIDs[relDirection].label;
                auto adjList = copier->directionLabelAdjLists[relDirection][nodeLabel].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *copier->directionLabelListSizes[relDirection][nodeLabel],
                    nodeIDs[relDirection].offset, 1);
                adjList->setElement(adjList->getListHeadersBuilder()->getHeader(nodeOffset),
                    nodeOffset, reversePos[relDirection], (uint8_t*)(&nodeIDs[!relDirection]));
            }
        }
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoLists(numPropertiesToRead, copier->directionLabelPropertyLists,
                copier->directionLabelAdjLists, copier->relLabel->properties,
                copier->propertyListsOverflowFiles, overflowPagesCursors, reader, nodeIDs,
                reversePos);
        }
        putValueIntoLists(copier->relLabel->getRelIDDefinition().propertyID,
            copier->directionLabelPropertyLists, copier->directionLabelAdjLists, nodeIDs,
            reversePos, (uint8_t*)&relID);
        relID++;
    }
    copier->logger->trace("End: path=`{0}` blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemRelCSVCopier::copyStringOverflowFromUnorderedToOrderedPages(gf_string_t* gfStr,
    PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
    InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile) {
    if (gfStr->len > gf_string_t::SHORT_STR_LENGTH) {
        TypeUtils::decodeOverflowPtr(gfStr->overflowPtr, unorderedOverflowCursor.pageIdx,
            unorderedOverflowCursor.offsetInPage);
        orderedOverflowFile->copyStringOverflow(orderedOverflowCursor,
            unorderedOverflowFile->getPage(unorderedOverflowCursor.pageIdx)->data +
                unorderedOverflowCursor.offsetInPage,
            gfStr);
    }
}

void InMemRelCSVCopier::copyListOverflowFromUnorderedToOrderedPages(gf_list_t* gfList,
    const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        gfList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflow(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, gfList, dataType.childType.get());
}

void InMemRelCSVCopier::sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
    InMemOverflowFile* unorderedOverflowPages, InMemOverflowFile* orderedOverflowPages) {
    PageByteCursor unorderedOverflowCursor, orderedOverflowCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        if (dataType.typeID == STRING) {
            auto gfStr = reinterpret_cast<gf_string_t*>(propertyColumn->getElement(offsetStart));
            copyStringOverflowFromUnorderedToOrderedPages(gfStr, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedOverflowPages, orderedOverflowPages);
        } else if (dataType.typeID == LIST) {
            auto gfList = reinterpret_cast<gf_list_t*>(propertyColumn->getElement(offsetStart));
            copyListOverflowFromUnorderedToOrderedPages(gfList, dataType, unorderedOverflowCursor,
                orderedOverflowCursor, unorderedOverflowPages, orderedOverflowPages);
        } else {
            assert(false);
        }
    }
}

void InMemRelCSVCopier::sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
    InMemLists* propertyLists, InMemOverflowFile* unorderedOverflowPages,
    InMemOverflowFile* orderedOverflowPages) {
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
                auto gfStr = reinterpret_cast<gf_string_t*>(propertyLists->getMemPtrToLoc(
                    propertyListCursor.pageIdx, propertyListCursor.posInPage));
                copyStringOverflowFromUnorderedToOrderedPages(gfStr, unorderedOverflowCursor,
                    orderedOverflowCursor, unorderedOverflowPages, orderedOverflowPages);
            } else if (dataType.typeID == LIST) {
                auto gfList = reinterpret_cast<gf_list_t*>(propertyLists->getMemPtrToLoc(
                    propertyListCursor.pageIdx, propertyListCursor.posInPage));
                copyListOverflowFromUnorderedToOrderedPages(gfList, dataType,
                    unorderedOverflowCursor, orderedOverflowCursor, unorderedOverflowPages,
                    orderedOverflowPages);
            } else {
                assert(false);
            }
        }
    }
}

void InMemRelCSVCopier::sortOverflowValues() {
    for (auto relDirection : REL_DIRECTIONS) {
        // Sort overflow values of property lists.
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relLabel->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    assert(propertyListsOverflowFiles[property.propertyID]);
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyList = directionLabelPropertyLists[relDirection]
                                                .at(nodeLabel)[property.propertyID]
                                                .get();
                        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                            sortOverflowValuesOfPropertyListsTask, property.dataType, offsetStart,
                            offsetEnd, adjList.get(), propertyList,
                            propertyListsOverflowFiles[property.propertyID].get(),
                            propertyList->getOverflowPages()));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    propertyListsOverflowFiles.clear();
    // Sort overflow values of property columns.
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [nodeLabel, column] : directionLabelPropertyColumns[relDirection]) {
            auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : relLabel->properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    assert(propertyColumnsOverflowFiles[property.propertyID]);
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyColumn = directionLabelPropertyColumns[relDirection]
                                                  .at(nodeLabel)[property.propertyID]
                                                  .get();
                        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                            sortOverflowValuesOfPropertyColumnTask, property.dataType, offsetStart,
                            offsetEnd, propertyColumn,
                            propertyColumnsOverflowFiles[property.propertyID].get(),
                            propertyColumn->getOverflowPages()));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    propertyColumnsOverflowFiles.clear();
}

void InMemRelCSVCopier::saveToFile() {
    logger->debug("Writing columns and lists to disk for rel {}.", relLabel->labelName);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels = catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
            relLabel->labelID, relDirection);
        // Write columns
        for (auto& [_, adjColumn] : directionLabelAdjColumns[relDirection]) {
            adjColumn->saveToFile();
        }
        for (auto& [_, propertyColumns] : directionLabelPropertyColumns[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < relLabel->getNumProperties(); propertyIdx++) {
                propertyColumns[propertyIdx]->saveToFile();
            }
        }
        // Write lists
        for (auto& [_, adjList] : directionLabelAdjLists[relDirection]) {
            adjList->saveToFile();
        }
        for (auto& [_, propertyLists] : directionLabelPropertyLists[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < relLabel->getNumProperties(); propertyIdx++) {
                propertyLists[propertyIdx]->saveToFile();
            }
        }
    }
    logger->debug("Done writing columns and lists to disk for rel {}.", relLabel->labelName);
}

} // namespace storage
} // namespace graphflow
