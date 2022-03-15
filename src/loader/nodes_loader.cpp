#include "src/loader/include/nodes_loader.h"

#include "src/common/include/exception.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

NodesLoader::NodesLoader(TaskScheduler& taskScheduler, const Graph& graph, string outputDirectory,
    vector<NodeFileDescription>& nodeFileDescriptions, LoaderProgressBar* progressBar)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      taskScheduler{taskScheduler}, graph{graph}, outputDirectory{std::move(outputDirectory)},
      fileDescriptions{nodeFileDescriptions}, progressBar{progressBar} {
    logger->debug("Initializing NodesLoader.");
};

void NodesLoader::load(const vector<string>& filePaths, const vector<uint64_t>& numBlocksPerLabel,
    const vector<vector<uint64_t>>& numLinesPerBlock, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    labelUnstrPropertyListsSizes.resize(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (!graph.getCatalog().getUnstructuredNodeProperties(nodeLabel).empty()) {
            labelUnstrPropertyListsSizes[nodeLabel] =
                make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    // If n is the number of node labels/files, we add n jobs to progress bar for the jobs for
    // constructing the in-memory property columns of each node file.
    for (label_t nodeLabel = 0; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        NodeLabelDescription description(nodeLabel, fileDescriptions[nodeLabel].filePath,
            numBlocksPerLabel[nodeLabel], graph.getCatalog().getStructuredNodeProperties(nodeLabel),
            fileDescriptions[nodeLabel].csvReaderConfig, nodeIDMaps[nodeLabel].get());
        InMemNodePropertyColumnsBuilder builder{description, taskScheduler, graph, outputDirectory};
        constructPropertyColumnsAndCountUnstrProperties(
            description, numLinesPerBlock[nodeLabel], builder);
    }
    // Currently, we do not use NodeFileDescription for construction of unstrPropertyLists. Rather,
    // we still rely on the old code. Ultimately this code path too should be using
    // NodeLabelDescriptions.
    constructUnstrPropertyLists(filePaths, numBlocksPerLabel, numLinesPerBlock);
}

// Constructs PropertyColumn for structured properties of all node labels and also counts the number
// of unstructured property and their sizes for each node. The task is executed in parallel by
// calling populatePropertyColumnsAndCountUnstrPropertyListSizesTask(...) on each block of all
// labels.
void NodesLoader::constructPropertyColumnsAndCountUnstrProperties(NodeLabelDescription& description,
    const vector<uint64_t>& numLinesPerBlock, InMemNodePropertyColumnsBuilder& builder) {
    logger->debug("Populating PropertyColumns and Counting unstructured properties.");
    node_offset_t offsetStart = 0;
    progressBar->addAndStartNewJob("Constructing property columns for node: " +
                                       graph.getCatalog().getNodeLabelName(description.label),
        description.numBlocks);
    for (auto blockIdx = 0u; blockIdx < description.numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            populatePropertyColumnsAndCountUnstrPropertyListSizesTask, &description, blockIdx,
            offsetStart, description.nodeIDMap, &builder,
            labelUnstrPropertyListsSizes[description.label].get(), logger, progressBar));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done populating PropertyColumns and Counting unstructured properties.");
    builder.saveToFile(progressBar);
}

// Constructs unstructured PropertyLists for the labels that have at least one unstructured
// property. The task is executed in parallel by calling populateUnstrPropertyListsTask(...) on each
// block of all labels.
void NodesLoader::constructUnstrPropertyLists(const vector<string>& filePaths,
    const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock) {
    buildUnstrPropertyListsHeadersAndMetadata();
    buildInMemUnstrPropertyLists();
    logger->debug("Populating Unstructured Property Lists.");
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto unstrPropertiesNameToIdMap =
            graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel);
        if (!unstrPropertiesNameToIdMap.empty()) {
            node_offset_t offsetStart = 0;
            auto listSizes = labelUnstrPropertyListsSizes[nodeLabel].get();
            auto& listsHeaders = labelUnstrPropertyListHeaders[nodeLabel];
            auto& listsMetadata = labelUnstrPropertyListsMetadata[nodeLabel];
            auto unstrPropertyLists = labelUnstrPropertyLists[nodeLabel].get();
            progressBar->addAndStartNewJob("Constructing unstructured property lists for node: " +
                                               fileDescriptions[nodeLabel].labelName,
                numBlocksPerLabel[nodeLabel]);
            for (auto blockIdx = 0u; blockIdx < numBlocksPerLabel[nodeLabel]; blockIdx++) {
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    populateUnstrPropertyListsTask, filePaths[nodeLabel], blockIdx,
                    fileDescriptions[nodeLabel].csvReaderConfig,
                    graph.getCatalog().getStructuredNodeProperties(nodeLabel).size(), offsetStart,
                    unstrPropertiesNameToIdMap, listSizes, &listsHeaders, &listsMetadata,
                    unstrPropertyLists, labelUnstrPropertyListsStringOverflowPages[nodeLabel].get(),
                    logger, progressBar));
                offsetStart += numLinesPerBlock[nodeLabel][blockIdx];
            }
            taskScheduler.waitAllTasksToCompleteOrError();
        }
    }
    saveUnstrPropertyListsToFile();
    logger->debug("Done populating Unstructured Property Lists.");
}

// Initializes ListHeaders and ListsMetadata auxiliary structures for all unstructured
// PropertyLists.
void NodesLoader::buildUnstrPropertyListsHeadersAndMetadata() {
    labelUnstrPropertyListHeaders.resize(graph.getCatalog().getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (!graph.getCatalog().getUnstructuredNodeProperties(nodeLabel).empty()) {
            labelUnstrPropertyListHeaders[nodeLabel].init(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    labelUnstrPropertyListsMetadata.resize(graph.getCatalog().getNodeLabelsCount());
    logger->debug("Initializing UnstructuredPropertyListHeaders.");
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (!graph.getCatalog().getUnstructuredNodeProperties(nodeLabel).empty()) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                ListsUtils::calculateListHeadersTask, graph.getNumNodesPerLabel()[nodeLabel], 1,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel], logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing UnstructuredPropertyListHeaders.");
    logger->debug("Initializing UnstructuredPropertyListsMetadata.");
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (!graph.getCatalog().getUnstructuredNodeProperties(nodeLabel).empty()) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                ListsUtils::calculateListsMetadataTask, graph.getNumNodesPerLabel()[nodeLabel], 1,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel],
                &labelUnstrPropertyListsMetadata[nodeLabel], false /*hasNULLBytes*/, logger));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done initializing UnstructuredPropertyListsMetadata.");
}

void NodesLoader::buildInMemUnstrPropertyLists() {
    labelUnstrPropertyLists.resize(graph.getCatalog().getNodeLabelsCount());
    labelUnstrPropertyListsStringOverflowPages.resize(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (!graph.getCatalog().getUnstructuredNodeProperties(nodeLabel).empty()) {
            auto unstrPropertyListsFName =
                NodesStore::getNodeUnstrPropertyListsFName(outputDirectory, nodeLabel);
            labelUnstrPropertyLists[nodeLabel] = make_unique<InMemUnstrPropertyPages>(
                unstrPropertyListsFName, labelUnstrPropertyListsMetadata[nodeLabel].numPages);
            labelUnstrPropertyListsStringOverflowPages[nodeLabel] = make_unique<InMemOverflowPages>(
                OverflowPages::getOverflowPagesFName(unstrPropertyListsFName));
        }
    }
}

void NodesLoader::saveUnstrPropertyListsToFile() {
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](InMemUnstrPropertyPages* x) { x->saveToFile(); },
                labelUnstrPropertyLists[nodeLabel].get()));
            taskScheduler.scheduleTask(
                LoaderTaskFactory::createLoaderTask([&](InMemOverflowPages* x) { x->saveToFile(); },
                    labelUnstrPropertyListsStringOverflowPages[nodeLabel].get()));
            auto fName = NodesStore::getNodeUnstrPropertyListsFName(outputDirectory, nodeLabel);
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                &labelUnstrPropertyListsMetadata[nodeLabel], fName));
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](ListHeaders* x, const string& fName) { x->saveToDisk(fName); },
                &labelUnstrPropertyListHeaders[nodeLabel], fName));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset and call the
// parser that reads structured properties of that line and puts in appropriate buffers. Also infers
// the size of list needed to store unstructured properties of the current node. Finally, dump the
// buffers to appropriate locations in files on disk.
void NodesLoader::populatePropertyColumnsAndCountUnstrPropertyListSizesTask(
    NodeLabelDescription* description, uint64_t blockId, node_offset_t offsetStart,
    NodeIDMap* nodeIDMap, InMemNodePropertyColumnsBuilder* builder,
    listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger>& logger,
    LoaderProgressBar* progressBar) {
    logger->trace("Start: path={0} blkIdx={1}", description->fName, blockId);
    vector<PageByteCursor> stringOverflowPagesCursors{description->properties.size()};
    CSVReader reader(description->fName, description->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line.
        }
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        if (!description->properties.empty()) {
            putPropsOfLineIntoInMemPropertyColumns(description->properties, reader, *builder,
                stringOverflowPagesCursors, nodeIDMap, offsetStart + bufferOffset);
        }
        calcLengthOfUnstrPropertyLists(reader, offsetStart + bufferOffset, *unstrPropertyListSizes);
        bufferOffset++;
    }
    progressBar->incrementTaskFinished();
    logger->trace("End: path={0} blkIdx={1}", description->fName, blockId);
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset, skip
// structured properties and calls the parser that reads unstructured properties of that line
// and puts in unstrPropertyLists.
void NodesLoader::populateUnstrPropertyListsTask(const string& fName, uint64_t blockId,
    const CSVReaderConfig& csvReaderConfig, uint32_t numStructuredProperties,
    node_offset_t offsetStart, const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    listSizes_t* unstrPropertyListSizes, ListHeaders* unstrPropertyListHeaders,
    ListsMetadata* unstrPropertyListsMetadata, InMemUnstrPropertyPages* unstrPropertyPages,
    InMemOverflowPages* stringOverflowPages, shared_ptr<spdlog::logger>& logger,
    LoaderProgressBar* progressBar) {
    logger->trace("Start: path={0} blkIdx={1}", fName, blockId);
    CSVReader reader(fName, csvReaderConfig, blockId);
    PageByteCursor stringOvfPagesCursor;
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        for (auto i = 0u; i < numStructuredProperties; ++i) {
            reader.hasNextToken();
        }
        putUnstrPropsOfALineToLists(reader, offsetStart + bufferOffset, unstrPropertiesNameToIdMap,
            *unstrPropertyListSizes, *unstrPropertyListHeaders, *unstrPropertyListsMetadata,
            *unstrPropertyPages, *stringOverflowPages, stringOvfPagesCursor);
        bufferOffset++;
    }
    progressBar->incrementTaskFinished();
    logger->trace("End: path={0} blkIdx={1}", fName, blockId);
}

// Calculates the size of unstructured property lists by iterating over unstructured properties
// on a line and deducing the size needed to store each property based on its dataType.
void NodesLoader::calcLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto startPos = strchr(unstrPropertyString, ':') + 1;
        *strchr(startPos, ':') = 0;
        ListsUtils::incrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN +
                TypeUtils::getDataTypeSize(TypeUtils::getDataType(string(startPos))));
    }
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value into appropriate buffer.
void NodesLoader::putPropsOfLineIntoInMemPropertyColumns(
    const vector<PropertyDefinition>& properties, CSVReader& reader,
    InMemNodePropertyColumnsBuilder& builder, vector<PageByteCursor>& stringOverflowPagesCursors,
    NodeIDMap* nodeIDMap, uint64_t nodeOffset) {
    for (const auto& property : properties) {
        if (property.dataType == UNSTRUCTURED) {
            continue;
        }
        reader.hasNextToken();
        switch (property.dataType) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto int64Val = reader.getInt64();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&int64Val), INT64);
                if (property.isPrimaryKey) {
                    nodeIDMap->set(to_string(int64Val).c_str(), nodeOffset);
                }
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&doubleVal), DOUBLE);
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&boolVal), BOOL);
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&dateVal), DATE);
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&timestampVal), TIMESTAMP);
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                builder.setProperty(
                    nodeOffset, property.id, reinterpret_cast<uint8_t*>(&intervalVal), INTERVAL);
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                if (strlen(strVal) > DEFAULT_PAGE_SIZE) {
                    throw LoaderException(StringUtils::string_format(
                        "Maximum length of strings is %d. Input string's length is %d.",
                        DEFAULT_PAGE_SIZE, strlen(strVal), strVal));
                }
                builder.setStringProperty(
                    nodeOffset, property.id, strVal, stringOverflowPagesCursors[property.id]);
                if (property.isPrimaryKey) {
                    nodeIDMap->set(strVal, nodeOffset);
                }
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(property.childDataType);
                builder.setListProperty(
                    nodeOffset, property.id, listVal, stringOverflowPagesCursors[property.id]);
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

// Parses the line by converting each unstructured property value to the dataType that is
// specified with that property puts the value at appropriate location in unstrPropertyPages.
void NodesLoader::putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
    const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    listSizes_t& unstrPropertyListSizes, ListHeaders& unstrPropertyListHeaders,
    ListsMetadata& unstrPropertyListsMetadata, InMemUnstrPropertyPages& unstrPropertyPages,
    InMemOverflowPages& stringOverflowPages, PageByteCursor& stringOvfPagesCursor) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
        *unstrPropertyStringBreaker1 = 0;
        auto propertyKeyId = unstrPropertiesNameToIdMap.at(string(unstrPropertyString));
        auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
        *unstrPropertyStringBreaker2 = 0;
        auto dataType = TypeUtils::getDataType(string(unstrPropertyStringBreaker1 + 1));
        auto dataTypeSize = TypeUtils::getDataTypeSize(dataType);
        auto reversePos = ListsUtils::decrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        PageElementCursor pageElementCursor;
        ListsUtils::calcPageElementCursor(unstrPropertyListHeaders.headers[nodeOffset], reversePos,
            1, nodeOffset, pageElementCursor, unstrPropertyListsMetadata, false /*hasNULLBytes*/);
        PageByteCursor pageCursor{pageElementCursor.idx, pageElementCursor.pos};
        char* valuePtr = unstrPropertyStringBreaker2 + 1;
        switch (dataType) {
        case INT64: {
            auto intVal = TypeUtils::convertToInt64(valuePtr);
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&intVal));
        } break;
        case DOUBLE: {
            auto doubleVal = TypeUtils::convertToDouble(valuePtr);
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&doubleVal));
        } break;
        case BOOL: {
            auto boolVal = TypeUtils::convertToBoolean(valuePtr);
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&boolVal));
        } break;
        case DATE: {
            char* beginningOfDateStr = valuePtr;
            date_t dateVal = Date::FromCString(beginningOfDateStr, strlen(beginningOfDateStr));
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&dateVal));
        } break;
        case TIMESTAMP: {
            char* beginningOfTimestampStr = valuePtr;
            timestamp_t timestampVal =
                Timestamp::FromCString(beginningOfTimestampStr, strlen(beginningOfTimestampStr));
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&timestampVal));
        } break;
        case INTERVAL: {
            char* beginningOfIntervalStr = valuePtr;
            interval_t intervalVal =
                Interval::FromCString(beginningOfIntervalStr, strlen(beginningOfIntervalStr));
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&intervalVal));
        } break;
        case STRING: {
            auto encodedString = stringOverflowPages.addString(valuePtr, stringOvfPagesCursor);
            unstrPropertyPages.set(pageCursor, propertyKeyId, static_cast<uint8_t>(dataType),
                dataTypeSize, reinterpret_cast<uint8_t*>(&encodedString));
        } break;
            // todo(Guodong): LIST for unstructured
        default:
            throw invalid_argument("unsupported dataType while parsing unstructured property");
        }
    }
}

} // namespace loader
} // namespace graphflow
