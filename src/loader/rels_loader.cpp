#include "src/loader/include/rels_loader.h"

namespace graphflow {
namespace loader {

RelsLoader::RelsLoader(ThreadPool& threadPool, Graph& graph, string outputDirectory,
    char tokenSeparator, vector<unique_ptr<NodeIDMap>>& nodeIDMaps)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")}, threadPool{threadPool}, graph{graph},
      outputDirectory(std::move(outputDirectory)), tokenSeparator{tokenSeparator},
      nodeIDMaps{nodeIDMaps} {}

// For each rel label, constructs the RelLabelDescription object with relevant meta info and
// calls the loadRelsForLabel.
void RelsLoader::load(const vector<string>& filePaths, vector<uint64_t>& numBlocksPerFile) {
    for (auto relLabel = 0u; relLabel < graph.catalog->getRelLabelsCount(); relLabel++) {
        RelLabelDescription description(graph.getCatalog().getRelProperties(relLabel));
        description.label = relLabel;
        description.fName = filePaths[relLabel];
        description.numBlocks = numBlocksPerFile[relLabel];
        for (auto direction : DIRECTIONS) {
            description.isSingleMultiplicityPerDirection[direction] =
                graph.catalog->isSingleMultiplicityInDirection(description.label, direction);
            description.nodeLabelsPerDirection[direction] =
                graph.catalog->getNodeLabelsForRelLabelDirection(description.label, direction);
        }
        for (auto direction : DIRECTIONS) {
            description.nodeIDCompressionSchemePerDirection[direction] =
                NodeIDCompressionScheme(description.nodeLabelsPerDirection[!direction],
                    graph.getNumNodesPerLabel(), graph.getCatalog().getNodeLabelsCount());
        }
        loadRelsForLabel(description);
    }
}

// Does 2 (1 mandatory, other optional) passes over the rel label's CSV file, one in each:
// 1. constructAdjColumnsAndCountRelsInAdjLists(...)
// 2. constructAdjLists(...), only if Lists are needed, i.e., rel label relMultiplicity is not 1.
void RelsLoader::loadRelsForLabel(RelLabelDescription& description) {
    logger->debug("Processing relLabel {}.", description.label);
    AdjAndPropertyListsBuilder adjAndPropertyListsBuilder{
        description, threadPool, graph, outputDirectory};
    constructAdjColumnsAndCountRelsInAdjLists(description, adjAndPropertyListsBuilder);
    if (!description.isSingleMultiplicityPerDirection[FWD] ||
        !description.isSingleMultiplicityPerDirection[BWD]) {
        constructAdjLists(description, adjAndPropertyListsBuilder);
    }
    logger->debug("Done processing relLabel {}.", description.label);
}

// Constructs AdjCoulmns and RelPropertyColumns if rel label have single relMultiplicity either in
// the FWD or BWD direction. Else if the relMultiplicity is not 1, counts the number of edges in
// each list in src/dst nodes. Uses AdjAndPropertyColumnsBuilder object to populate data in the
// in-memory pages. The task is executed in parallel by calling
// populateAdjColumnsAndCountRelsInAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjColumnsAndCountRelsInAdjLists(
    RelLabelDescription& description, AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder) {
    AdjAndPropertyColumnsBuilder adjAndPropertyColumnsBuilder{
        description, threadPool, graph, outputDirectory};
    logger->debug("Populating AdjColumns and Rel Property Columns.");
    auto& catalog = graph.getCatalog();
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjColumnsAndCountRelsInAdjListsTask, &description, blockId,
            tokenSeparator, &adjAndPropertyListsBuilder, &adjAndPropertyColumnsBuilder, &nodeIDMaps,
            &catalog, logger);
    }
    threadPool.wait();
    populateNumRels(adjAndPropertyColumnsBuilder, adjAndPropertyListsBuilder);
    logger->debug("Done populating AdjColumns and Rel Property Columns.");
    if (description.hasProperties() && !description.requirePropertyLists()) {
        adjAndPropertyColumnsBuilder.sortOverflowStrings();
    }
    adjAndPropertyColumnsBuilder.saveToFile();
}

void RelsLoader::populateNumRels(AdjAndPropertyColumnsBuilder& adjAndPropertyColumnsBuilder,
    AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder) {
    graph.numRelsPerDirBoundLabelRelLabel.resize(2);
    for (auto direction : DIRECTIONS) {
        graph.numRelsPerDirBoundLabelRelLabel[direction].resize(
            graph.getCatalog().getNodeLabelsCount());
        for (auto i = 0u; i < graph.getCatalog().getNodeLabelsCount(); i++) {
            graph.numRelsPerDirBoundLabelRelLabel[direction][i].resize(
                graph.getCatalog().getRelLabelsCount(), 0);
        }
    }
    adjAndPropertyColumnsBuilder.populateNumRelsInfo(graph.numRelsPerDirBoundLabelRelLabel, true);
    adjAndPropertyListsBuilder.populateNumRelsInfo(graph.numRelsPerDirBoundLabelRelLabel, false);
}

// Constructs AdjLists and RelPropertyLists if rel label does not have single relMultiplicity. For
// each Lists, the function uses AdjAndPropertyListsBuilder object to build corresponding
// listHeaders and metadata and populate data in the in-memory pages. Executes in parallel by
// calling populateAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjLists(
    RelLabelDescription& description, AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder) {
    adjAndPropertyListsBuilder.buildAdjListsHeadersAndListsMetadata();
    adjAndPropertyListsBuilder.buildInMemStructures();
    logger->debug("Populating AdjLists and Rel Property Lists.");
    auto& catalog = graph.getCatalog();
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjListsTask, &description, blockId, tokenSeparator,
            &adjAndPropertyListsBuilder, &nodeIDMaps, &catalog, logger);
    }
    threadPool.wait();
    logger->debug("Done populating AdjLists and Rel Property Lists.");

    if (description.requirePropertyLists()) {
        adjAndPropertyListsBuilder.sortOverflowStrings();
    }
    adjAndPropertyListsBuilder.saveToFile();
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel. If any of the relMultiplicity of rel label is single directional, puts the
// nbr nodeIDs to appropriate AdjColumns, else increment the size of the appropraite list. Also
// calls the parser that reads properties in a line and puts in appropraite PropertyColumns.
void RelsLoader::populateAdjColumnsAndCountRelsInAdjListsTask(RelLabelDescription* description,
    uint64_t blockId, char tokenSeparator, AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    AdjAndPropertyColumnsBuilder* adjAndPropertyColumnsBuilder,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", description->fName, blockId);
    CSVReader reader(description->fName, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<PageCursor> stringOverflowPagesCursors{description->properties.size()};
    for (auto& direction : DIRECTIONS) {
        requireToReadLabels[direction] = 1 != description->nodeLabelsPerDirection[direction].size();
        nodeIDs[direction].label =
            description->nodeLabelsPerDirection[direction].begin().operator*();
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& direction : DIRECTIONS) {
            if (description->isSingleMultiplicityPerDirection[direction]) {
                adjAndPropertyColumnsBuilder->setRel(direction, nodeIDs);
            } else {
                adjAndPropertyListsBuilder->incrementListSize(direction, nodeIDs[direction]);
            }
        }
        if (description->hasProperties() && !description->requirePropertyLists()) {
            if (description->isSingleMultiplicityPerDirection[FWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->properties, reader,
                    adjAndPropertyColumnsBuilder, nodeIDs[FWD], stringOverflowPagesCursors);
            } else if (description->isSingleMultiplicityPerDirection[BWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->properties, reader,
                    adjAndPropertyColumnsBuilder, nodeIDs[BWD], stringOverflowPagesCursors);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", description->fName, blockId);
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel and puts in AdjLists. Also calls the parser that reads properties in a line
// and puts in appropraite PropertyLists.
void RelsLoader::populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
    char tokenSeparator, AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", description->fName, blockId);
    CSVReader reader(description->fName, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<uint64_t> reversePos{2};
    vector<PageCursor> stringOverflowPagesCursors{description->properties.size()};
    for (auto& direction : DIRECTIONS) {
        requireToReadLabels[direction] = 1 != description->nodeLabelsPerDirection[direction].size();
        nodeIDs[direction].label =
            description->nodeLabelsPerDirection[direction].begin().operator*();
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& direction : DIRECTIONS) {
            if (!description->isSingleMultiplicityPerDirection[direction]) {
                reversePos[direction] =
                    adjAndPropertyListsBuilder->decrementListSize(direction, nodeIDs[direction]);
                adjAndPropertyListsBuilder->setRel(reversePos[direction], direction, nodeIDs);
            }
        }
        if (description->requirePropertyLists()) {
            putPropsOfLineIntoInMemRelPropLists(description->properties, reader, nodeIDs,
                reversePos, adjAndPropertyListsBuilder, stringOverflowPagesCursors);
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", description->fName, blockId);
}

void RelsLoader::inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    vector<bool>& requireToReadLabels) {
    for (auto& direction : DIRECTIONS) {
        reader.hasNextToken();
        if (requireToReadLabels[direction]) {
            nodeIDs[direction].label = (*catalog).getNodeLabelFromString(reader.getString());
        } else {
            reader.skipToken();
        }
        reader.hasNextToken();
        nodeIDs[direction].offset =
            (*(*nodeIDMaps)[nodeIDs[direction].label]).get(reader.getString());
    }
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value in appropriate propertyColumns.
void RelsLoader::putPropsOfLineIntoInMemPropertyColumns(
    const vector<PropertyDefinition>& properties, CSVReader& reader,
    AdjAndPropertyColumnsBuilder* adjAndPropertyColumnsBuilder, const nodeID_t& nodeID,
    vector<PageCursor>& stringOverflowPagesCursors) {
    auto propertyIdx = 0u;
    while (reader.hasNextToken()) {
        switch (properties[propertyIdx].dataType) {
        case INT64: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT64 : reader.getInt64();
            adjAndPropertyColumnsBuilder->setProperty(
                nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT64);
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
            adjAndPropertyColumnsBuilder->setProperty(
                nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&doubleVal), DOUBLE);
            break;
        }
        case BOOL: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
            adjAndPropertyColumnsBuilder->setProperty(
                nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&boolVal), BOOL);
            break;
        }
        case DATE: {
            auto dateVal = reader.skipTokenIfNull() ? NULL_DATE : reader.getDate();
            adjAndPropertyColumnsBuilder->setProperty(
                nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&dateVal), DATE);
            break;
        }
        case STRING: {
            auto strVal =
                reader.skipTokenIfNull() ? &gf_string_t::EMPTY_STRING : reader.getString();
            adjAndPropertyColumnsBuilder->setStringProperty(
                nodeID, propertyIdx, strVal, stringOverflowPagesCursors[propertyIdx]);
            break;
        }
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
    AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    vector<PageCursor>& stringOverflowPagesCursors) {
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch (properties[propertyIdx].dataType) {
        case INT64: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT64 : reader.getInt64();
            adjAndPropertyListsBuilder->setProperty(
                pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT64);
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
            adjAndPropertyListsBuilder->setProperty(
                pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&doubleVal), DOUBLE);
            break;
        }
        case BOOL: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
            adjAndPropertyListsBuilder->setProperty(
                pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&boolVal), BOOL);
            break;
        }
        case DATE: {
            auto dateVal = reader.skipTokenIfNull() ? NULL_DATE : reader.getDate();
            adjAndPropertyListsBuilder->setProperty(
                pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&dateVal), DATE);
            break;
        }
        case STRING: {
            auto strVal =
                reader.skipTokenIfNull() ? &gf_string_t::EMPTY_STRING : reader.getString();
            adjAndPropertyListsBuilder->setStringProperty(
                pos, nodeIDs, propertyIdx, strVal, stringOverflowPagesCursors[propertyIdx]);
            break;
        }
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
