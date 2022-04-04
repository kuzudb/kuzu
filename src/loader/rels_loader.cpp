#include "src/loader/include/rels_loader.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace loader {

RelsLoader::RelsLoader(TaskScheduler& taskScheduler, Catalog& catalog, string outputDirectory,
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps, const vector<RelFileDescription>& fileMetadata,
    LoaderProgressBar* progressBar)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      taskScheduler{taskScheduler}, catalog{catalog},
      outputDirectory(std::move(outputDirectory)), nodeIDMaps{nodeIDMaps},
      fileDescriptions(fileMetadata), progressBar{progressBar} {}

// For each rel label, constructs the RelLabelDescription object with relevant meta info and
// calls the loadRelsForLabel.
void RelsLoader::load(vector<uint64_t>& numBlocksPerFile) {
    for (auto relLabel = 0u; relLabel < catalog.getNumRelLabels(); relLabel++) {
        RelLabelDescription description(relLabel, fileDescriptions[relLabel].filePath,
            numBlocksPerFile[relLabel], catalog.getRelProperties(relLabel),
            fileDescriptions[relLabel].csvReaderConfig);
        for (auto direction : REL_DIRECTIONS) {
            description.isSingleMultiplicityPerDirection[direction] =
                catalog.isSingleMultiplicityInDirection(description.label, direction);
            description.nodeLabelsPerDirection[direction] =
                catalog.getNodeLabelsForRelLabelDirection(description.label, direction);
            description.nodeIDCompressionSchemePerDirection[!direction] =
                NodeIDCompressionScheme(description.nodeLabelsPerDirection[direction],
                    catalog.getNumNodesPerLabel(), catalog.getNumNodeLabels());
        }
        loadRelsForLabel(description);
    }
}

// Does 2 (1 mandatory, other optional) passes over the rel label's CSV file, one in each:
// 1. constructAdjColumnsAndCountRelsInAdjLists(...)
// 2. constructAdjLists(...), only if Lists are needed, i.e., rel label relMultiplicity is not 1.
void RelsLoader::loadRelsForLabel(RelLabelDescription& description) {
    logger->debug("Processing relLabel {}.", description.label);
    InMemAdjAndPropertyListsBuilder listsBuilder{
        description, taskScheduler, catalog, outputDirectory};
    constructAdjColumnsAndCountRelsInAdjLists(description, listsBuilder);
    if (!description.isSingleMultiplicityPerDirection[FWD] ||
        !description.isSingleMultiplicityPerDirection[BWD]) {
        constructAdjLists(description, listsBuilder);
    }
    logger->debug("Done processing relLabel {}.", description.label);
}

// Constructs AdjColumns and RelPropertyColumns if rel label have single relMultiplicity either in
// the FWD or BWD direction. Else if the relMultiplicity is not 1, counts the number of edges in
// each list in src/dst nodes. Uses InMemAdjAndPropertyColumnsBuilder object to populate data in the
// in-memory pages. The task is executed in parallel by calling
// populateAdjColumnsAndCountRelsInAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjColumnsAndCountRelsInAdjLists(
    RelLabelDescription& description, InMemAdjAndPropertyListsBuilder& listsBuilder) {
    InMemAdjAndPropertyColumnsBuilder columnsBuilder{
        description, taskScheduler, catalog, outputDirectory};
    logger->debug("Populating AdjColumns and Rel Property Columns.");
    progressBar->addAndStartNewJob(
        "Populating adjacency columns and counting relations for relationship: " +
            catalog.getRelLabelName(description.label),
        description.numBlocks);
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            populateAdjColumnsAndCountRelsInAdjListsTask, &description, blockId, &listsBuilder,
            &columnsBuilder, &nodeIDMaps, &catalog, logger, progressBar));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    populateNumRels(description, columnsBuilder, listsBuilder);
    logger->debug("Done populating AdjColumns and Rel Property Columns.");
    if (description.hasProperties() && !description.requirePropertyLists()) {
        columnsBuilder.sortOverflowStrings(progressBar);
    }
    columnsBuilder.saveToFile(progressBar);
}

void RelsLoader::populateNumRels(const RelLabelDescription& description,
    InMemAdjAndPropertyColumnsBuilder& columnsBuilder,
    InMemAdjAndPropertyListsBuilder& listsBuilder) {
    auto& rel = catalog.getRel(description.label);
    for (auto relDirection : REL_DIRECTIONS) {
        if (description.isSingleMultiplicityPerDirection[relDirection]) {
            for (auto boundNodeLabel : description.nodeLabelsPerDirection[relDirection]) {
                rel.numRelsPerDirectionBoundLabel[relDirection].emplace(boundNodeLabel,
                    columnsBuilder.getNumRelsForDirectionLabel(relDirection, boundNodeLabel));
            }
        } else {
            for (auto boundNodeLabel : description.nodeLabelsPerDirection[relDirection]) {
                rel.numRelsPerDirectionBoundLabel[relDirection].emplace(boundNodeLabel,
                    listsBuilder.getNumRelsForDirectionLabel(relDirection, boundNodeLabel));
            }
        }
    }
}

// Constructs AdjLists and RelPropertyLists if rel label does not have single relMultiplicity. For
// each Lists, the function uses InMemAdjAndPropertyListsBuilder object to build corresponding
// listHeaders and metadata and populate data in the in-memory pages. Executes in parallel by
// calling populateAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjLists(
    RelLabelDescription& description, InMemAdjAndPropertyListsBuilder& listsBuilder) {
    listsBuilder.buildAdjListsHeadersAndListsMetadata(progressBar);
    listsBuilder.buildInMemStructures();
    logger->debug("Populating AdjLists and Rel Property Lists.");
    progressBar->addAndStartNewJob("Constructing adjacency lists for relationship: " +
                                       catalog.getRelLabelName(description.label),
        description.numBlocks);
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(populateAdjListsTask,
            &description, blockId, description.csvReaderConfig, &listsBuilder, &nodeIDMaps,
            &catalog, logger, progressBar));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done populating AdjLists and Rel Property Lists.");

    if (description.requirePropertyLists()) {
        listsBuilder.sortOverflowStrings(progressBar);
    }
    listsBuilder.saveToFile(progressBar);
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel. If any of the relMultiplicity of rel label is single directional, puts the
// nbr nodeIDs to appropriate AdjColumns, else increment the size of the appropriate list. Also
// calls the parser that reads properties in a line and puts in appropriate PropertyColumns.
void RelsLoader::populateAdjColumnsAndCountRelsInAdjListsTask(RelLabelDescription* description,
    uint64_t blockId, InMemAdjAndPropertyListsBuilder* listsBuilder,
    InMemAdjAndPropertyColumnsBuilder* columnsBuilder, vector<unique_ptr<NodeIDMap>>* nodeIDMaps,
    const Catalog* catalog, shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar) {
    logger->debug("Start: path=`{0}` blkIdx={1}", description->fName, blockId);
    CSVReader reader(description->fName, description->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<PageByteCursor> stringOverflowPagesCursors{description->properties.size()};
    for (auto& direction : REL_DIRECTIONS) {
        requireToReadLabels[direction] = 1 != description->nodeLabelsPerDirection[direction].size();
        nodeIDs[direction].label =
            description->nodeLabelsPerDirection[direction].begin().operator*();
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& direction : REL_DIRECTIONS) {
            if (description->isSingleMultiplicityPerDirection[direction]) {
                columnsBuilder->setRel(direction, nodeIDs);
            } else {
                listsBuilder->incrementListSize(direction, nodeIDs[direction]);
            }
        }
        if (description->hasProperties() && !description->requirePropertyLists()) {
            if (description->isSingleMultiplicityPerDirection[FWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->properties, reader,
                    columnsBuilder, nodeIDs[FWD], stringOverflowPagesCursors);
            } else if (description->isSingleMultiplicityPerDirection[BWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->properties, reader,
                    columnsBuilder, nodeIDs[BWD], stringOverflowPagesCursors);
            }
        }
    }
    logger->debug("End: path=`{0}` blkIdx={1}", description->fName, blockId);
    progressBar->incrementTaskFinished();
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel and puts in AdjLists. Also calls the parser that reads properties in a line
// and puts in appropriate PropertyLists.
void RelsLoader::populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
    const CSVReaderConfig& csvReaderConfig, InMemAdjAndPropertyListsBuilder* listsBuilder,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar) {
    logger->trace("Start: path=`{0}` blkIdx={1}", description->fName, blockId);
    CSVReader reader(description->fName, csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs;
    nodeIDs.reserve(2);
    vector<uint64_t> reversePos;
    reversePos.reserve(2);
    vector<PageByteCursor> overflowPagesCursors{description->properties.size()};
    for (auto& direction : REL_DIRECTIONS) {
        requireToReadLabels[direction] = 1 != description->nodeLabelsPerDirection[direction].size();
        nodeIDs[direction].label =
            description->nodeLabelsPerDirection[direction].begin().operator*();
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& direction : REL_DIRECTIONS) {
            if (!description->isSingleMultiplicityPerDirection[direction]) {
                reversePos[direction] =
                    listsBuilder->decrementListSize(direction, nodeIDs[direction]);
                listsBuilder->setRel(reversePos[direction], direction, nodeIDs);
            }
        }
        if (description->requirePropertyLists()) {
            putPropsOfLineIntoInMemRelPropLists(description->properties, reader, nodeIDs,
                reversePos, listsBuilder, overflowPagesCursors);
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", description->fName, blockId);
    progressBar->incrementTaskFinished();
}

void RelsLoader::inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    vector<bool>& requireToReadLabels) {
    for (auto& direction : REL_DIRECTIONS) {
        reader.hasNextToken();
        if (requireToReadLabels[direction]) {
            nodeIDs[direction].label = (*catalog).getNodeLabelFromName(reader.getString());
        } else {
            reader.skipToken();
        }
        reader.hasNextToken();
        auto offsetStr = reader.getString();
        try {
            nodeIDs[direction].offset = (*(*nodeIDMaps)[nodeIDs[direction].label]).get(offsetStr);
        } catch (const std::out_of_range& e) {
            throw LoaderException(string(e.what()) + " nodeOffset: " + offsetStr);
        }
    }
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value in appropriate propertyColumns.
void RelsLoader::putPropsOfLineIntoInMemPropertyColumns(
    const vector<PropertyDefinition>& properties, CSVReader& reader,
    InMemAdjAndPropertyColumnsBuilder* columnsBuilder, const nodeID_t& nodeID,
    vector<PageByteCursor>& stringOverflowPagesCursors) {
    auto propertyIdx = 0u;
    while (reader.hasNextToken()) {
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto intVal = reader.getInt64();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT64);
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&doubleVal), DOUBLE);
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&boolVal), BOOL);
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&dateVal), DATE);
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&timestampVal), TIMESTAMP);
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                columnsBuilder->setProperty(
                    nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&intervalVal), INTERVAL);
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                columnsBuilder->setStringProperty(
                    nodeID, propertyIdx, strVal, stringOverflowPagesCursors[propertyIdx]);
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                columnsBuilder->setListProperty(
                    nodeID, propertyIdx, listVal, stringOverflowPagesCursors[propertyIdx]);
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
        propertyIdx++;
    }
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value in appropriate propertyLists.
void RelsLoader::putPropsOfLineIntoInMemRelPropLists(const vector<PropertyDefinition>& properties,
    CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& pos,
    InMemAdjAndPropertyListsBuilder* listsBuilder, vector<PageByteCursor>& overflowPagesCursors) {
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch (properties[propertyIdx].dataType.typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto intVal = reader.getInt64();
                listsBuilder->setProperty(
                    pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT64);
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {

                auto doubleVal = reader.getDouble();
                listsBuilder->setProperty(
                    pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&doubleVal), DOUBLE);
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                listsBuilder->setProperty(
                    pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&boolVal), BOOL);
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                listsBuilder->setProperty(
                    pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&dateVal), DATE);
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                listsBuilder->setProperty(pos, nodeIDs, propertyIdx,
                    reinterpret_cast<uint8_t*>(&timestampVal), TIMESTAMP);
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                listsBuilder->setProperty(
                    pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&intervalVal), INTERVAL);
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                listsBuilder->setStringProperty(
                    pos, nodeIDs, propertyIdx, strVal, overflowPagesCursors[propertyIdx]);
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*properties[propertyIdx].dataType.childType);
                listsBuilder->setListProperty(
                    pos, nodeIDs, propertyIdx, listVal, overflowPagesCursors[propertyIdx]);
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
        propertyIdx++;
    }
}

} // namespace loader
} // namespace graphflow
