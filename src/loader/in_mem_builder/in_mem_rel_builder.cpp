#include "src/loader/in_mem_builder/include/in_mem_rel_builder.h"

#include "spdlog/spdlog.h"

#include "src/loader/include/loader_task.h"

namespace graphflow {
namespace loader {

InMemRelBuilder::InMemRelBuilder(label_t label, const RelFileDescription& fileDescription,
    string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog,
    const vector<uint64_t>& maxNodeOffsetsPerNodeLabel, uint64_t startRelID,
    BufferManager& bufferManager, LoaderProgressBar* progressBar)
    : InMemStructuresBuilder{label, fileDescription.labelName, fileDescription.filePath,
          move(outputDirectory), fileDescription.csvReaderConfig, taskScheduler, catalog,
          progressBar},
      relMultiplicity{getRelMultiplicityFromString(fileDescription.relMultiplicity)},
      srcNodeLabelNames{fileDescription.srcNodeLabelNames},
      dstNodeLabelNames{fileDescription.dstNodeLabelNames},
      maxNodeOffsetsPerNodeLabel{maxNodeOffsetsPerNodeLabel},
      startRelID{startRelID}, bm{bufferManager} {
    tmpReadTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    IDIndexes.resize(catalog.getNumNodeLabels());
    unordered_set<string> labelNames;
    labelNames.insert(
        fileDescription.srcNodeLabelNames.begin(), fileDescription.srcNodeLabelNames.end());
    labelNames.insert(
        fileDescription.dstNodeLabelNames.begin(), fileDescription.dstNodeLabelNames.end());
    for (auto& labelName : labelNames) {
        auto nodeLabel = catalog.getNodeLabelFromName(labelName);
        assert(IDIndexes[nodeLabel] == nullptr);
        IDIndexes[nodeLabel] = make_unique<HashIndex>(
            StorageUtils::getNodeIndexIDAndFName(this->outputDirectory, nodeLabel),
            catalog.getNodeProperty(nodeLabel, LoaderConfig::ID_FIELD).dataType, bm,
            true /* isInMemory */);
    }
}

uint64_t InMemRelBuilder::load() {
    logger->info("Loading rel {} with label {}.", labelName, label);
    vector<PropertyNameDataType> propertyDefinitions;
    numBlocks = parseHeaderAndChunkFile(inputFilePath, propertyDefinitions);
    countLinesPerBlock();
    catalog.addRelLabel(labelName, relMultiplicity, move(propertyDefinitions), srcNodeLabelNames,
        dstNodeLabelNames);
    auto& relLabel = catalog.getRel(labelName);
    relLabel.addRelIDDefinition();
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
    logger->info("Done loading rel {} with label {}.", labelName, label);
    auto numRels = 0u;
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        numRels += numLinesPerBlock[blockId];
    }
    numRels--; // Decrement the header line.
    return numRels;
}

void InMemRelBuilder::countLinesPerBlock() {
    logger->info("Counting number of lines in each block");
    numLinesPerBlock.resize(numBlocks);
    progressBar->addAndStartNewJob("Counting lines in the file for rel: " + labelName, numBlocks);
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            countNumLinesPerBlockTask, inputFilePath, blockId, this));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Done counting number of lines in each block.");
}

void InMemRelBuilder::initializeColumnsAndLists() {
    for (auto relDirection : REL_DIRECTIONS) {
        directionNodeIDCompressionScheme[relDirection] =
            NodeIDCompressionScheme(catalog.getNodeLabelsForRelLabelDirection(label, !relDirection),
                maxNodeOffsetsPerNodeLabel);
        directionNumRelsPerLabel[relDirection] =
            make_unique<atomic_uint64_vec_t>(catalog.getNumNodeLabels());
        directionLabelListSizes[relDirection].resize(catalog.getNumNodeLabels());
        for (auto nodeLabel = 0u; nodeLabel < catalog.getNumNodeLabels(); nodeLabel++) {
            directionLabelListSizes[relDirection][nodeLabel] = make_unique<atomic_uint64_vec_t>(
                maxNodeOffsetsPerNodeLabel[nodeLabel] + 1 /* num nodes */);
        }
        if (catalog.isSingleMultiplicityInDirection(label, relDirection)) {
            initializeColumns(relDirection);
        } else {
            initializeLists(relDirection);
        }
    }
    auto properties = catalog.getRelProperties(label);
    propertyListsOverflowFiles.resize(properties.size());
    propertyColumnsOverflowFiles.resize(properties.size());
    for (auto& property : properties) {
        if (property.dataType.typeID == LIST || property.dataType.typeID == STRING) {
            propertyListsOverflowFiles[property.propertyID] = make_unique<InMemOverflowFile>();
            propertyColumnsOverflowFiles[property.propertyID] = make_unique<InMemOverflowFile>();
        }
    }
}

void InMemRelBuilder::initializeColumns(RelDirection relDirection) {
    auto nodeLabels = catalog.getNodeLabelsForRelLabelDirection(label, relDirection);
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
        directionLabelAdjColumns[relDirection].emplace(nodeLabel,
            make_unique<InMemAdjColumn>(
                StorageUtils::getAdjColumnFName(outputDirectory, label, nodeLabel, relDirection),
                directionNodeIDCompressionScheme[relDirection], numNodes));
        auto properties = catalog.getRelProperties(label);
        vector<unique_ptr<InMemColumn>> propertyColumns(properties.size());
        for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, label, nodeLabel, relDirection, properties[propertyIdx].name);
            propertyColumns[propertyIdx] = InMemColumnFactory::getInMemPropertyColumn(
                fName, properties[propertyIdx].dataType, numNodes);
        }
        directionLabelPropertyColumns[relDirection].emplace(nodeLabel, move(propertyColumns));
    }
}

void InMemRelBuilder::initializeLists(RelDirection relDirection) {
    auto nodeLabels = catalog.getNodeLabelsForRelLabelDirection(label, relDirection);
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
        directionLabelAdjLists[relDirection].emplace(nodeLabel,
            make_unique<InMemAdjLists>(
                StorageUtils::getAdjListsFName(outputDirectory, label, nodeLabel, relDirection),
                directionNodeIDCompressionScheme[relDirection], numNodes));
        auto properties = catalog.getRelProperties(label);
        vector<unique_ptr<InMemLists>> propertyLists(properties.size());
        for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
            auto fName = StorageUtils::getRelPropertyListsFName(
                outputDirectory, label, nodeLabel, relDirection, properties[propertyIdx].name);
            propertyLists[propertyIdx] = InMemListsFactory::getInMemPropertyLists(
                fName, properties[propertyIdx].dataType, numNodes);
        }
        directionLabelPropertyLists[relDirection].emplace(nodeLabel, move(propertyLists));
    }
}

void InMemRelBuilder::populateAdjColumnsAndCountRelsInAdjLists() {
    logger->info("Populating adj columns and rel property columns for rel {}.", labelName);
    progressBar->addAndStartNewJob(
        "Populating adjacency columns and counting relations for relationship: " +
            catalog.getRelLabelName(label),
        numBlocks);
    auto blockStartOffset = 0ull;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(
            LoaderTaskFactory::createLoaderTask(populateAdjColumnsAndCountRelsInAdjListsTask,
                blockIdx, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    populateNumRels();
    logger->info("Done populating adj columns and rel property columns for rel {}.", labelName);
}

static void putValueIntoColumns(uint64_t propertyIdx,
    vector<label_property_columns_map_t>& directionLabelPropertyColumns,
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

void InMemRelBuilder::populateAdjColumnsAndCountRelsInAdjListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelBuilder* builder) {
    builder->logger->debug("Start: path=`{0}` blkIdx={1}", builder->inputFilePath, blockId);
    CSVReader reader(builder->inputFilePath, builder->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodeIDTypes{2};
    auto properties = builder->catalog.getRelProperties(builder->label);
    for (auto& relDirection : REL_DIRECTIONS) {
        auto nodeLabels =
            builder->catalog.getNodeLabelsForRelLabelDirection(builder->label, relDirection);
        requireToReadLabels[relDirection] = nodeLabels.size() != 1;
        nodeIDs[relDirection].label = *nodeLabels.begin();
        nodeIDTypes[relDirection] =
            builder->catalog.getNodeProperty(nodeIDs[relDirection].label, LoaderConfig::ID_FIELD)
                .dataType;
    }
    vector<PageByteCursor> overflowPagesCursors{properties.size()};
    auto& relLabel = builder->catalog.getRel(builder->label);
    auto numPropertiesToRead = relLabel.getNumPropertiesToReadFromCSV();
    auto& relIDDefinition = relLabel.getRelIDDefinition();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDTypes, builder->IDIndexes,
            builder->tmpReadTransaction.get(), builder->catalog, requireToReadLabels);
        for (auto relDirection : REL_DIRECTIONS) {
            auto nodeLabel = nodeIDs[relDirection].label;
            auto nodeOffset = nodeIDs[relDirection].offset;
            if (builder->directionLabelAdjColumns[relDirection].contains(nodeLabel)) {
                builder->directionLabelAdjColumns[relDirection].at(nodeLabel)->setElement(
                    nodeOffset, (uint8_t*)&nodeIDs[!relDirection]);
            } else {
                InMemListsUtils::incrementListSize(
                    *builder->directionLabelListSizes[relDirection].at(nodeLabel), nodeOffset, 1);
            }
            builder->directionNumRelsPerLabel[relDirection]->operator[](nodeLabel)++;
        }
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoColumns(numPropertiesToRead, builder->directionLabelPropertyColumns,
                builder->catalog.getRelProperties(builder->label),
                builder->propertyColumnsOverflowFiles, overflowPagesCursors, reader, nodeIDs);
        }
        putValueIntoColumns(relIDDefinition.propertyID, builder->directionLabelPropertyColumns,
            nodeIDs, (uint8_t*)&relID);
        relID++;
    }
    builder->logger->debug("End: path=`{0}` blkIdx={1}", builder->inputFilePath, blockId);
    builder->progressBar->incrementTaskFinished();
}

void InMemRelBuilder::putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
    vector<label_property_columns_map_t>& directionLabelPropertyColumns,
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

void InMemRelBuilder::inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<DataType>& nodeIDTypes, const vector<unique_ptr<HashIndex>>& IDIndexes,
    Transaction* transaction, const Catalog& catalog, vector<bool>& requireToReadLabels) {
    for (auto& relDirection : REL_DIRECTIONS) {
        reader.hasNextToken();
        if (requireToReadLabels[relDirection]) {
            nodeIDs[relDirection].label = catalog.getNodeLabelFromName(reader.getString());
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
                throw LoaderException("Cannot find key: " + to_string(key) + " in the IDIndex.");
            }
        } break;
        case STRING: {
            if (!IDIndexes[nodeIDs[relDirection].label]->lookup(
                    transaction, keyStr, nodeIDs[relDirection].offset)) {
                throw LoaderException("Cannot find key: " + string(keyStr) + " in the IDIndex.");
            }
        } break;
        default:
            throw LoaderException("Unsupported data type " +
                                  Types::dataTypeToString(nodeIDTypes[relDirection]) +
                                  " for index lookup.");
        }
    }
}

static void putValueIntoLists(uint64_t propertyIdx,
    vector<label_property_lists_map_t>& directionLabelPropertyLists,
    vector<label_adj_lists_map_t>& directionLabelAdjLists, const vector<nodeID_t>& nodeIDs,
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

void InMemRelBuilder::putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
    vector<label_property_lists_map_t>& directionLabelPropertyLists,
    vector<label_adj_lists_map_t>& directionLabelAdjLists, const vector<Property>& properties,
    vector<unique_ptr<InMemOverflowFile>>& overflowPages, vector<PageByteCursor>& overflowCursors,
    CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos) {
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

void InMemRelBuilder::populateNumRels() {
    auto& rel = catalog.getRel(label);
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto boundNodeLabel : catalog.getNodeLabelsForRelLabelDirection(label, relDirection)) {
            rel.numRelsPerDirectionBoundLabel[relDirection].emplace(boundNodeLabel,
                directionNumRelsPerLabel[relDirection]->operator[](boundNodeLabel).load());
        }
    }
}

void InMemRelBuilder::initAdjListsHeaders() {
    logger->debug("Initializing AdjListHeaders for rel {}.", labelName);
    progressBar->addAndStartNewJob(
        "Initializing adjacency lists headers for relationship: " + labelName,
        directionLabelAdjLists[FWD].size() + directionLabelAdjLists[BWD].size());
    for (auto relDirection : REL_DIRECTIONS) {
        auto numBytesPerNode = directionNodeIDCompressionScheme[relDirection].getNumTotalBytes();
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(calculateListHeadersTask,
                maxNodeOffsetsPerNodeLabel[nodeLabel] + 1, numBytesPerNode,
                directionLabelListSizes[relDirection][nodeLabel].get(),
                adjList->getListHeadersBuilder(), logger, progressBar));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    progressBar->incrementTaskFinished();
    logger->debug("Done initializing AdjListHeaders for rel {}.", labelName);
}

uint64_t InMemRelBuilder::getNumTasksOfInitializingAdjAndPropertyListsMetadata() {
    auto numTasks = 0;
    auto numProperties = catalog.getRelProperties(label).size();
    for (auto relDirection : REL_DIRECTIONS) {
        numTasks += directionLabelAdjLists[relDirection].size() +
                    directionLabelAdjLists[relDirection].size() * numProperties;
    }
    return numTasks;
}

void InMemRelBuilder::initAdjAndPropertyListsMetadata() {
    logger->debug("Initializing adjLists and propertyLists metadata for rel {}.", labelName);
    progressBar->addAndStartNewJob(
        "Initializing adjacency and property lists metadata for rel: " + labelName,
        getNumTasksOfInitializingAdjAndPropertyListsMetadata());
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
            auto listSizes = directionLabelListSizes[relDirection][nodeLabel].get();
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                directionNodeIDCompressionScheme[relDirection].getNumTotalBytes(), listSizes,
                adjList->getListHeadersBuilder(), adjList.get(), false /*hasNULLBytes*/, logger,
                progressBar));
            for (auto& property : catalog.getRelProperties(label)) {
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    calculateListsMetadataAndAllocateInMemListPagesTask, numNodes,
                    Types::getDataTypeSize(property.dataType), listSizes,
                    adjList->getListHeadersBuilder(),
                    directionLabelPropertyLists[relDirection][nodeLabel][property.propertyID].get(),
                    true /*hasNULLBytes*/, logger, progressBar));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing adjLists and propertyLists metadata for rel {}.", labelName);
}

void InMemRelBuilder::populateAdjAndPropertyLists() {
    logger->debug("Populating adjLists and rel property lists for rel {}.", labelName);
    progressBar->addAndStartNewJob(
        "Populating adjacency lists for relationship: " + catalog.getRelLabelName(label),
        numBlocks);
    auto blockStartOffset = 0ull;
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            populateAdjAndPropertyListsTask, blockId, startRelID + blockStartOffset, this));
        blockStartOffset += numLinesPerBlock[blockId];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done populating adjLists and rel property lists for rel {}.", labelName);
}

void InMemRelBuilder::populateAdjAndPropertyListsTask(
    uint64_t blockId, uint64_t blockStartRelID, InMemRelBuilder* builder) {
    builder->logger->trace("Start: path=`{0}` blkIdx={1}", builder->inputFilePath, blockId);
    CSVReader reader(builder->inputFilePath, builder->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<DataType> nodeIDTypes{2};
    vector<uint64_t> reversePos{2};
    auto properties = builder->catalog.getRelProperties(builder->label);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels =
            builder->catalog.getNodeLabelsForRelLabelDirection(builder->label, relDirection);
        requireToReadLabels[relDirection] = nodeLabels.size() != 1;
        nodeIDs[relDirection].label = *nodeLabels.begin();
        nodeIDTypes[relDirection] =
            builder->catalog.getNodeProperty(nodeIDs[relDirection].label, LoaderConfig::ID_FIELD)
                .dataType;
    }
    vector<PageByteCursor> overflowPagesCursors(properties.size());
    auto& relLabel = builder->catalog.getRel(builder->label);
    auto numPropertiesToRead = relLabel.getNumPropertiesToReadFromCSV();
    auto& relIDDefinition = relLabel.getRelIDDefinition();
    int64_t relID = blockStartRelID;
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDTypes, builder->IDIndexes,
            builder->tmpReadTransaction.get(), builder->catalog, requireToReadLabels);
        for (auto relDirection : REL_DIRECTIONS) {
            if (!builder->catalog.isSingleMultiplicityInDirection(builder->label, relDirection)) {
                auto nodeOffset = nodeIDs[relDirection].offset;
                auto nodeLabel = nodeIDs[relDirection].label;
                auto adjList = builder->directionLabelAdjLists[relDirection][nodeLabel].get();
                reversePos[relDirection] = InMemListsUtils::decrementListSize(
                    *builder->directionLabelListSizes[relDirection][nodeLabel],
                    nodeIDs[relDirection].offset, 1);
                adjList->setElement(adjList->getListHeadersBuilder()->getHeader(nodeOffset),
                    nodeOffset, reversePos[relDirection], (uint8_t*)(&nodeIDs[!relDirection]));
            }
        }
        if (numPropertiesToRead != 0) {
            putPropsOfLineIntoLists(numPropertiesToRead, builder->directionLabelPropertyLists,
                builder->directionLabelAdjLists, builder->catalog.getRelProperties(builder->label),
                builder->propertyListsOverflowFiles, overflowPagesCursors, reader, nodeIDs,
                reversePos);
        }
        putValueIntoLists(relIDDefinition.propertyID, builder->directionLabelPropertyLists,
            builder->directionLabelAdjLists, nodeIDs, reversePos, (uint8_t*)&relID);
        relID++;
    }
    builder->logger->trace("End: path=`{0}` blkIdx={1}", builder->inputFilePath, blockId);
    builder->progressBar->incrementTaskFinished();
}

void InMemRelBuilder::copyStringOverflowFromUnorderedToOrderedPages(gf_string_t* gfStr,
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

void InMemRelBuilder::copyListOverflowFromUnorderedToOrderedPages(gf_list_t* gfList,
    const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
    PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
    InMemOverflowFile* orderedOverflowFile) {
    TypeUtils::decodeOverflowPtr(
        gfList->overflowPtr, unorderedOverflowCursor.pageIdx, unorderedOverflowCursor.offsetInPage);
    orderedOverflowFile->copyListOverflow(unorderedOverflowFile, unorderedOverflowCursor,
        orderedOverflowCursor, gfList, dataType.childType.get());
}

void InMemRelBuilder::sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
    InMemOverflowFile* unorderedOverflowPages, InMemOverflowFile* orderedOverflowPages,
    LoaderProgressBar* progressBar) {
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
    progressBar->incrementTaskFinished();
}

void InMemRelBuilder::sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
    InMemLists* propertyLists, InMemOverflowFile* unorderedOverflowPages,
    InMemOverflowFile* orderedOverflowPages, LoaderProgressBar* progressBar) {
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
    progressBar->incrementTaskFinished();
}

void InMemRelBuilder::sortOverflowValues() {
    auto properties = catalog.getRelProperties(label);
    for (auto relDirection : REL_DIRECTIONS) {
        // Sort overflow values of property lists.
        for (auto& [nodeLabel, adjList] : directionLabelAdjLists[relDirection]) {
            auto numNodes = maxNodeOffsetsPerNodeLabel[nodeLabel] + 1;
            auto numBuckets = numNodes / 256;
            numBuckets += (numNodes % 256 != 0);
            for (auto& property : properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    assert(propertyListsOverflowFiles[property.propertyID]);
                    progressBar->addAndStartNewJob(
                        "Sorting overflow buckets for property: " + labelName + "." + property.name,
                        numBuckets);
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyList = directionLabelPropertyLists[relDirection]
                                                .at(nodeLabel)[property.propertyID]
                                                .get();
                        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                            sortOverflowValuesOfPropertyListsTask, property.dataType, offsetStart,
                            offsetEnd, adjList.get(), propertyList,
                            propertyListsOverflowFiles[property.propertyID].get(),
                            propertyList->getOverflowPages(), progressBar));
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
            for (auto& property : properties) {
                if (property.dataType.typeID == STRING || property.dataType.typeID == LIST) {
                    assert(propertyColumnsOverflowFiles[property.propertyID]);
                    progressBar->addAndStartNewJob(
                        "Sorting overflow buckets for property: " + labelName + "." + property.name,
                        numBuckets);
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        auto propertyColumn = directionLabelPropertyColumns[relDirection]
                                                  .at(nodeLabel)[property.propertyID]
                                                  .get();
                        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                            sortOverflowValuesOfPropertyColumnTask, property.dataType, offsetStart,
                            offsetEnd, propertyColumn,
                            propertyColumnsOverflowFiles[property.propertyID].get(),
                            propertyColumn->getOverflowPages(), progressBar));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    propertyColumnsOverflowFiles.clear();
}

void InMemRelBuilder::saveToFile() {
    logger->debug("Writing columns and lists to disk for rel {}.", labelName);
    progressBar->addAndStartNewJob("Writing adj columns and lists to disk.", 1);
    auto properties = catalog.getRelProperties(label);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels = catalog.getNodeLabelsForRelLabelDirection(label, relDirection);
        // Write columns
        for (auto& [_, adjColumn] : directionLabelAdjColumns[relDirection]) {
            adjColumn->saveToFile();
        }
        for (auto& [_, propertyColumns] : directionLabelPropertyColumns[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
                propertyColumns[propertyIdx]->saveToFile();
            }
        }
        // Write lists
        for (auto& [_, adjList] : directionLabelAdjLists[relDirection]) {
            adjList->saveToFile();
        }
        for (auto& [_, propertyLists] : directionLabelPropertyLists[relDirection]) {
            for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
                propertyLists[propertyIdx]->saveToFile();
            }
        }
    }
    logger->debug("Done writing columns and lists to disk for rel {}.", labelName);
}

} // namespace loader
} // namespace graphflow
