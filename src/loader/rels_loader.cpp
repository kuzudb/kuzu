#include "src/loader/include/rels_loader.h"

#include "src/loader/include/csv_reader.h"

namespace graphflow {
namespace loader {

// For each rel label, constructs the RelLabelDescription object with relevant meta info and
// calls the loadRelsForLabel.
void RelsLoader::load(vector<string>& fnames, vector<uint64_t>& numBlocksPerFile) {
    RelLabelDescription description;
    for (auto relLabel = 0u; relLabel < catalog.getRelLabelsCount(); relLabel++) {
        auto& propertyMap = catalog.getPropertyKeyMapForRelLabel(relLabel);
        description.propertyMap = &propertyMap;
        description.label = relLabel;
        description.fname = fnames[relLabel];
        description.numBlocks = numBlocksPerFile[relLabel];
        for (auto& dir : DIRS) {
            description.isSingleCardinalityPerDir[dir] =
                catalog.isSingleCaridinalityInDir(description.label, dir);
            description.nodeLabelsPerDir[dir] =
                catalog.getNodeLabelsForRelLabelDir(description.label, dir);
        }
        for (auto& dir : DIRS) {
            description.nodeIDCompressionSchemePerDir[dir] =
                NodeIDCompressionScheme(description.nodeLabelsPerDir[!dir],
                    graph.getNumNodesPerLabel(), catalog.getNodeLabelsCount());
        }
        description.propertyDataTypes = createPropertyDataTypesArray(propertyMap);
        loadRelsForLabel(description);
    }
}

// Does 2 (1 mandatory, other optional) passes over the rel label's CSV file, one in each:
// 1. constructAdjColumnsAndCountRelsInAdjLists(...)
// 2. constructAdjLists(...), only if Lists are needed, i.e., rel label cardinality is not 1.
void RelsLoader::loadRelsForLabel(RelLabelDescription& description) {
    logger->debug("Processing relLabel {}.", description.label);
    AdjAndPropertyListsBuilder adjAndPropertyListsBuilder{
        description, threadPool, graph, catalog, outputDirectory};
    constructAdjColumnsAndCountRelsInAdjLists(description, adjAndPropertyListsBuilder);
    if (!description.isSingleCardinalityPerDir[FWD] ||
        !description.isSingleCardinalityPerDir[BWD]) {
        constructAdjLists(description, adjAndPropertyListsBuilder);
    }
    logger->debug("Done processing relLabel {}.", description.label);
}

// Constructs AdjCoulmns and RelPropertyColumns if rel label have single cardinality either in
// the FWD or BWD direction. Else if the cardinality is not 1, counts the number of edges in
// each list in src/dst nodes. Uses AdjAndPropertyColumnsBuilder object to populate data in the
// in-memory pages. The task is executed in parallel by calling
// populateAdjColumnsAndCountRelsInAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjColumnsAndCountRelsInAdjLists(
    RelLabelDescription& description, AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder) {
    AdjAndPropertyColsBuilderAndListSizeCounter adjAndPropertyColumnsBuilder{
        description, threadPool, graph, catalog, outputDirectory};
    logger->debug("Populating AdjColumns and Rel Property Columns.");
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjColumnsAndCountRelsInAdjListsTask, &description, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &adjAndPropertyListsBuilder,
            &adjAndPropertyColumnsBuilder, &nodeIDMaps, &catalog, logger);
    }
    threadPool.wait();
    logger->debug("Done populating AdjColumns and Rel Property Columns.");
    if (description.hasProperties() && !description.requirePropertyLists()) {
        adjAndPropertyColumnsBuilder.sortOverflowStrings();
    }
    adjAndPropertyColumnsBuilder.saveToFile();
}

// Constructs AdjLists and RelPropertyLists if rel label does not have single cardinality. For
// each Lists, the function uses AdjAndPropertyListsBuilder object to build corresponding
// listHeaders and metadata and populate data in the in-memory pages. Executes in parallel by
// calling populateAdjListsTask(...) on each block of the CSV file.
void RelsLoader::constructAdjLists(
    RelLabelDescription& description, AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder) {
    adjAndPropertyListsBuilder.buildAdjListsHeadersAndListsMetadata();
    adjAndPropertyListsBuilder.buildInMemStructures();
    logger->debug("Populating AdjLists and Rel Property Lists.");
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjListsTask, &description, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &adjAndPropertyListsBuilder,
            &nodeIDMaps, &catalog, logger);
    }
    threadPool.wait();
    logger->debug("Done populating AdjLists and Rel Property Lists.");

    if (description.requirePropertyLists()) {
        adjAndPropertyListsBuilder.sortOverflowStrings();
    }
    adjAndPropertyListsBuilder.saveToFile();
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel. If any of the cardinality of rel label is single directional, puts the
// nbr nodeIDs to appropriate AdjColumns, else increment the size of the appropraite list. Also
// calls the parser that reads properties in a line and puts in appropraite PropertyColumns.
void RelsLoader::populateAdjColumnsAndCountRelsInAdjListsTask(RelLabelDescription* description,
    uint64_t blockId, const char tokenSeparator,
    AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    AdjAndPropertyColsBuilderAndListSizeCounter* adjAndPropertyColumnsBuilder,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", description->fname, blockId);
    CSVReader reader(description->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<PageCursor> stringOverflowPagesCursors{description->propertyMap->size()};
    for (auto& dir : DIRS) {
        requireToReadLabels[dir] = 1 != description->nodeLabelsPerDir[dir].size();
        nodeIDs[dir].label = description->nodeLabelsPerDir[dir][0];
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& dir : DIRS) {
            if (description->isSingleCardinalityPerDir[dir]) {
                adjAndPropertyColumnsBuilder->setRel(dir, nodeIDs);
            } else {
                adjAndPropertyListsBuilder->incrementListSize(dir, nodeIDs[dir]);
            }
        }
        if (description->hasProperties() && !description->requirePropertyLists()) {
            if (description->isSingleCardinalityPerDir[FWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->propertyDataTypes, reader,
                    adjAndPropertyColumnsBuilder, nodeIDs[FWD], stringOverflowPagesCursors, logger);
            } else if (description->isSingleCardinalityPerDir[BWD]) {
                putPropsOfLineIntoInMemPropertyColumns(description->propertyDataTypes, reader,
                    adjAndPropertyColumnsBuilder, nodeIDs[BWD], stringOverflowPagesCursors, logger);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", description->fname, blockId);
}

// Iterate over each line in a block of CSV file. For each line, infer the src/dest node labels and
// offsets of the rel and puts in AdjLists. Also calls the parser that reads properties in a line
// and puts in appropraite PropertyLists.
void RelsLoader::populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
    const char tokenSeparator, AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", description->fname, blockId);
    CSVReader reader(description->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<bool> requireToReadLabels{true, true};
    vector<nodeID_t> nodeIDs{2};
    vector<uint64_t> reversePos{2};
    vector<PageCursor> stringOverflowPagesCursors{description->propertyMap->size()};
    for (auto& dir : DIRS) {
        requireToReadLabels[dir] = 1 != description->nodeLabelsPerDir[dir].size();
        nodeIDs[dir].label = description->nodeLabelsPerDir[dir][0];
    }
    while (reader.hasNextLine()) {
        inferLabelsAndOffsets(reader, nodeIDs, nodeIDMaps, catalog, requireToReadLabels);
        for (auto& dir : DIRS) {
            if (!description->isSingleCardinalityPerDir[dir]) {
                reversePos[dir] = adjAndPropertyListsBuilder->decrementListSize(dir, nodeIDs[dir]);
                adjAndPropertyListsBuilder->setRel(reversePos[dir], dir, nodeIDs);
            }
        }
        if (description->requirePropertyLists()) {
            putPropsOfLineIntoInMemRelPropLists(description->propertyDataTypes, reader, nodeIDs,
                reversePos, adjAndPropertyListsBuilder, stringOverflowPagesCursors, logger);
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", description->fname, blockId);
}

void RelsLoader::inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
    vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
    vector<bool>& requireToReadLabels) {
    for (auto& dir : DIRS) {
        reader.hasNextToken();
        if (requireToReadLabels[dir]) {
            nodeIDs[dir].label = (*catalog).getNodeLabelFromString(reader.getString());
        } else {
            reader.skipToken();
        }
        reader.hasNextToken();
        nodeIDs[dir].offset = (*(*nodeIDMaps)[nodeIDs[dir].label]).get(reader.getString());
    }
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value in appropriate propertyColumns.
void RelsLoader::putPropsOfLineIntoInMemPropertyColumns(const vector<DataType>& propertyDataTypes,
    CSVReader& reader, AdjAndPropertyColsBuilderAndListSizeCounter* adjAndPropertyColumnsBuilder,
    const nodeID_t& nodeID, vector<PageCursor>& stringOverflowPagesCursors,
    shared_ptr<spdlog::logger> logger) {
    auto propertyIdx = 0u;
    while (reader.hasNextToken()) {
        switch (propertyDataTypes[propertyIdx]) {
        case INT32: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT32 : reader.getInt32();
            adjAndPropertyColumnsBuilder->setProperty(
                nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT32);
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
        case STRING: {
            auto strVal = reader.skipTokenIfNull() ? &EMPTY_STRING : reader.getString();
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
void RelsLoader::putPropsOfLineIntoInMemRelPropLists(const vector<DataType>& propertyDataTypes,
    CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& pos,
    AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
    vector<PageCursor>& stringOverflowPagesCursors, shared_ptr<spdlog::logger> logger) {
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch (propertyDataTypes[propertyIdx]) {
        case INT32: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT32 : reader.getInt32();
            adjAndPropertyListsBuilder->setProperty(
                pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&intVal), INT32);
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
        case STRING: {
            auto strVal = reader.skipTokenIfNull() ? &EMPTY_STRING : reader.getString();
            adjAndPropertyListsBuilder->setStringProperty(
                pos, nodeIDs, propertyIdx, strVal, stringOverflowPagesCursors[propertyIdx]);
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
