#include "src/loader/include/nodes_loader.h"

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/include/date.h"
#include "src/common/include/file_utils.h"
#include "src/storage/include/store/nodes_store.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

NodesLoader::NodesLoader(
    ThreadPool& threadPool, const Graph& graph, string outputDirectory, char tokenSeparator)
    : logger{spdlog::get("loader")}, threadPool{threadPool}, graph{graph},
      outputDirectory{std::move(outputDirectory)}, tokenSeparator{tokenSeparator} {
    logger->debug("Initializing NodesLoader.");
};

void NodesLoader::load(const vector<string>& filePaths, const vector<uint64_t>& numBlocksPerLabel,
    const vector<vector<uint64_t>>& numLinesPerBlock, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    labelUnstrPropertyListsSizes.resize(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            labelUnstrPropertyListsSizes[nodeLabel] =
                make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    constructPropertyColumnsAndCountUnstrProperties(
        filePaths, numBlocksPerLabel, numLinesPerBlock, nodeIDMaps);
    constructUnstrPropertyLists(filePaths, numBlocksPerLabel, numLinesPerBlock);
}

// Constructs PropertyColumn for structured properties of all node labels and also counts the number
// of unstructured property and their sizes for each node. The task is executed in parallel by
// calling populatePropertyColumnsAndCountUnstrPropertyListSizesTask(...) on each block of all
// labels.
void NodesLoader::constructPropertyColumnsAndCountUnstrProperties(const vector<string>& filePaths,
    const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock,
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    auto numNodeLabels = graph.getCatalog().getNodeLabelsCount();
    vector<vector<unique_ptr<InMemStringOverflowPages>>> labelPropertyIdxStringOverflowPages(
        numNodeLabels);
    vector<vector<string>> labelPropertyColumnFnames(numNodeLabels);
    for (label_t nodeLabel = 0u; nodeLabel < numNodeLabels; nodeLabel++) {
        auto properties = graph.getCatalog().getNodeProperties(nodeLabel);
        labelPropertyColumnFnames[nodeLabel].resize(properties.size());
        labelPropertyIdxStringOverflowPages[nodeLabel].resize(properties.size());
        for (const auto& property : properties) {
            labelPropertyColumnFnames[nodeLabel][property.id] =
                NodesStore::getNodePropertyColumnFname(outputDirectory, nodeLabel, property.name);
            if (STRING == property.dataType) {
                labelPropertyIdxStringOverflowPages[nodeLabel][property.id] =
                    make_unique<InMemStringOverflowPages>(
                        NodesStore::getStringNodePropertyOverflowFname(
                            outputDirectory, nodeLabel, property.name));
            }
        }
    }
    logger->debug("Populating PropertyColumns and Counting unstructured properties.");
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto properties = graph.getCatalog().getNodeProperties(nodeLabel);
        node_offset_t offsetStart = 0;
        for (auto blockIdx = 0u; blockIdx < numBlocksPerLabel[nodeLabel]; blockIdx++) {
            threadPool.execute(populatePropertyColumnsAndCountUnstrPropertyListSizesTask,
                filePaths[nodeLabel], blockIdx, tokenSeparator, properties,
                numLinesPerBlock[nodeLabel][blockIdx], offsetStart, nodeIDMaps[nodeLabel].get(),
                labelPropertyColumnFnames[nodeLabel],
                &labelPropertyIdxStringOverflowPages[nodeLabel],
                labelUnstrPropertyListsSizes[nodeLabel].get(), logger);
            offsetStart += numLinesPerBlock[nodeLabel][blockIdx];
        }
    }
    threadPool.wait();
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto& properties = graph.getCatalog().getNodeProperties(nodeLabel);
        for (const auto& property : properties) {
            if (STRING == property.dataType) {
                threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                    labelPropertyIdxStringOverflowPages[nodeLabel][property.id].get());
            }
        }
    }
    threadPool.wait();
    logger->debug("Done populating PropertyColumns and Counting unstructured properties.");
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
        auto numStructuredProperties = graph.getCatalog().getNodeProperties(nodeLabel).size() -
                                       unstrPropertiesNameToIdMap.size();
        if (!unstrPropertiesNameToIdMap.empty()) {
            node_offset_t offsetStart = 0;
            auto listSizes = labelUnstrPropertyListsSizes[nodeLabel].get();
            auto& listsHeaders = labelUnstrPropertyListHeaders[nodeLabel];
            auto& listsMetadata = labelUnstrPropertyListsMetadata[nodeLabel];
            auto unstrPropertyLists = labelUnstrPropertyLists[nodeLabel].get();
            for (auto blockIdx = 0u; blockIdx < numBlocksPerLabel[nodeLabel]; blockIdx++) {
                threadPool.execute(populateUnstrPropertyListsTask, filePaths[nodeLabel], blockIdx,
                    tokenSeparator, numStructuredProperties, offsetStart,
                    unstrPropertiesNameToIdMap, listSizes, &listsHeaders, &listsMetadata,
                    unstrPropertyLists, labelUnstrPropertyListsStringOverflowPages[nodeLabel].get(),
                    logger);
                offsetStart += numLinesPerBlock[nodeLabel][blockIdx];
            }
        }
    }
    threadPool.wait();
    saveUnstrPropertyListsToFile();
    logger->debug("Done populating Unstructured Property Lists.");
}

// Initializes ListHeaders and ListsMetadata auxiliary structures for all unstructured
// PropertyLists.
void NodesLoader::buildUnstrPropertyListsHeadersAndMetadata() {
    labelUnstrPropertyListHeaders.resize(graph.getCatalog().getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            labelUnstrPropertyListHeaders[nodeLabel].init(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    labelUnstrPropertyListsMetadata.resize(graph.getCatalog().getNodeLabelsCount());
    logger->debug("Initializing UnstructuredPropertyListHeaders.");
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            threadPool.execute(ListsLoaderHelper::calculateListHeadersTask,
                graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel], logger);
        }
    }
    threadPool.wait();
    logger->debug("Done initializing UnstructuredPropertyListHeaders.");
    logger->debug("Initializing UnstructuredPropertyListsMetadata.");
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
                graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel],
                &labelUnstrPropertyListsMetadata[nodeLabel], logger);
        }
    }
    threadPool.wait();
    logger->debug("Done initializing UnstructuredPropertyListsMetadata.");
}

void NodesLoader::buildInMemUnstrPropertyLists() {
    labelUnstrPropertyLists.resize(graph.getCatalog().getNodeLabelsCount());
    labelUnstrPropertyListsStringOverflowPages.resize(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            auto unstrPropertyListsFName =
                NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            labelUnstrPropertyLists[nodeLabel] = make_unique<InMemUnstrPropertyPages>(
                unstrPropertyListsFName, labelUnstrPropertyListsMetadata[nodeLabel].numPages);
            labelUnstrPropertyListsStringOverflowPages[nodeLabel] =
                make_unique<InMemStringOverflowPages>(
                    NodesStore::getStringOverflowFnameOfColumnOrList(unstrPropertyListsFName));
        }
    }
}

void NodesLoader::saveUnstrPropertyListsToFile() {
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertiesNameToIdMap(nodeLabel).size() > 0) {
            threadPool.execute([&](InMemUnstrPropertyPages* x) { x->saveToFile(); },
                labelUnstrPropertyLists[nodeLabel].get());
            threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                labelUnstrPropertyListsStringOverflowPages[nodeLabel].get());
            auto fName = NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            threadPool.execute([&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                &labelUnstrPropertyListsMetadata[nodeLabel], fName);
            threadPool.execute([&](ListHeaders* x, const string& fName) { x->saveToDisk(fName); },
                &labelUnstrPropertyListHeaders[nodeLabel], fName);
        }
    }
    threadPool.wait();
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset and call the
// parser that reads structured properties of that line and puts in appropriate buffers. Also infers
// the size of list needed to store unstructured properties of the current node. Finally, dump the
// buffers to appropriate locations in files on disk.
void NodesLoader::populatePropertyColumnsAndCountUnstrPropertyListSizesTask(const string& fName,
    uint64_t blockId, char tokenSeparator, const vector<PropertyDefinition>& properties,
    uint64_t numElements, node_offset_t offsetStart, NodeIDMap* nodeIDMap,
    const vector<string>& propertyColumnFNames,
    vector<unique_ptr<InMemStringOverflowPages>>* propertyIdxStringOverflowPages,
    listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger>& logger) {
    try {
        logger->trace("Start: path={0} blkIdx={1}", fName, blockId);
        vector<PageCursor> stringOverflowPagesCursors{properties.size()};
        auto buffers = createBuffersForProperties(properties, numElements, logger);
        CSVReader reader(fName, tokenSeparator, blockId);
        if (0 == blockId) {
            if (reader.hasNextLine()) {}
        }
        auto bufferOffset = 0u;
        while (reader.hasNextLine()) {
            if (!properties.empty()) {
                putPropsOfLineIntoBuffers(properties, reader, *buffers, bufferOffset,
                    *propertyIdxStringOverflowPages, stringOverflowPagesCursors, nodeIDMap,
                    offsetStart + bufferOffset);
            }
            calcLengthOfUnstrPropertyLists(
                reader, offsetStart + bufferOffset, *unstrPropertyListSizes);
            bufferOffset++;
        }
        writeBuffersToFiles(*buffers, offsetStart, numElements, propertyColumnFNames, properties);
        logger->trace("End: path={0} blkIdx={1}", fName, blockId);
    } catch (exception& e) {
        logger->error("Caught an exception during loading!!");
        logger->error(e.what());
        exit(1);
    }
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset, skip
// structured properties and calls the parser that reads unstructured properties of that line
// and puts in unstrPropertyLists.
void NodesLoader::populateUnstrPropertyListsTask(const string& fName, uint64_t blockId,
    char tokenSeparator, uint32_t numStructuredProperties, node_offset_t offsetStart,
    const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    listSizes_t* unstrPropertyListSizes, ListHeaders* unstrPropertyListHeaders,
    ListsMetadata* unstrPropertyListsMetadata, InMemUnstrPropertyPages* unstrPropertyPages,
    InMemStringOverflowPages* stringOverflowPages, shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: path={0} blkIdx={1}", fName, blockId);
    CSVReader reader(fName, tokenSeparator, blockId);
    PageCursor stringOvfPagesCursor;
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
        ListsLoaderHelper::incrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN +
                TypeUtils::getDataTypeSize(TypeUtils::getDataType(string(startPos))));
    }
}

unique_ptr<vector<unique_ptr<uint8_t[]>>> NodesLoader::createBuffersForProperties(
    const vector<PropertyDefinition>& properties, uint64_t numElements,
    shared_ptr<spdlog::logger>& logger) {
    logger->trace("Creating buffers of size {}", numElements);
    auto buffers = make_unique<vector<unique_ptr<uint8_t[]>>>(properties.size());
    for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
        if (properties[propertyIdx].dataType != UNSTRUCTURED) {
            (*buffers)[propertyIdx] = make_unique<uint8_t[]>(
                numElements * TypeUtils::getDataTypeSize(properties[propertyIdx].dataType));
        }
    };
    return buffers;
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value into appropriate buffer.
void NodesLoader::putPropsOfLineIntoBuffers(const vector<PropertyDefinition>& properties,
    CSVReader& reader, vector<unique_ptr<uint8_t[]>>& buffers, const uint32_t& bufferOffset,
    vector<unique_ptr<InMemStringOverflowPages>>& propertyIdxStringOverflowPages,
    vector<PageCursor>& stringOverflowPagesCursors, NodeIDMap* nodeIDMap, uint64_t nodeOffset) {
    for (auto propertyId = 0u; propertyId < properties.size(); ++propertyId) {
        auto property = properties[propertyId];
        if (property.dataType == UNSTRUCTURED) {
            continue;
        }
        reader.hasNextToken();
        switch (property.dataType) {
        case INT64: {
            auto int64Val = reader.skipTokenIfNull() ? NULL_INT64 : reader.getInt64();
            memcpy(buffers[propertyId].get() + (bufferOffset * TypeUtils::getDataTypeSize(INT64)),
                &int64Val, TypeUtils::getDataTypeSize(INT64));
            if (property.isPrimaryKey) {
                nodeIDMap->set(to_string(int64Val).c_str(), nodeOffset);
            }
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
            memcpy(buffers[propertyId].get() + (bufferOffset * TypeUtils::getDataTypeSize(DOUBLE)),
                &doubleVal, TypeUtils::getDataTypeSize(DOUBLE));
            break;
        }
        case BOOL: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
            memcpy(buffers[propertyId].get() + (bufferOffset * TypeUtils::getDataTypeSize(BOOL)),
                &boolVal, TypeUtils::getDataTypeSize(BOOL));
            break;
        }
        case DATE: {
            auto dateVal = reader.skipTokenIfNull() ? NULL_DATE : reader.getDate();
            memcpy(buffers[propertyId].get() + (bufferOffset * TypeUtils::getDataTypeSize(DATE)),
                &dateVal, TypeUtils::getDataTypeSize(DATE));
            break;
        }
        case STRING: {
            auto strVal =
                reader.skipTokenIfNull() ? &gf_string_t::EMPTY_STRING : reader.getString();
            auto encodedString = reinterpret_cast<gf_string_t*>(
                buffers[propertyId].get() + (bufferOffset * TypeUtils::getDataTypeSize(STRING)));
            propertyIdxStringOverflowPages[propertyId]->setStrInOvfPageAndPtrInEncString(
                strVal, stringOverflowPagesCursors[propertyId], encodedString);
            if (property.isPrimaryKey) {
                nodeIDMap->set(strVal, nodeOffset);
            }
            break;
        }
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void NodesLoader::writeBuffersToFiles(const vector<unique_ptr<uint8_t[]>>& buffers,
    const uint64_t& offsetStart, const uint64_t& numElementsToWrite,
    const vector<string>& propertyColumnFnames, const vector<PropertyDefinition>& properties) {
    for (auto propertyId = 0u; propertyId < properties.size(); propertyId++) {
        if (properties[propertyId].dataType == UNSTRUCTURED) {
            continue;
        }
        auto fd = FileUtils::openFile(propertyColumnFnames[propertyId], O_WRONLY | O_CREAT);
        auto offsetInFile =
            offsetStart * TypeUtils::getDataTypeSize(properties[propertyId].dataType);
        auto bytesToWrite =
            numElementsToWrite * TypeUtils::getDataTypeSize(properties[propertyId].dataType);
        FileUtils::writeToFile(fd, buffers[propertyId].get(), bytesToWrite, offsetInFile);
        FileUtils::closeFile(fd);
    }
}

// Parses the line by converting each unstructured property value to the dataType that is
// specified with that property puts the value at appropiate location in unstrPropertyPages.
void NodesLoader::putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
    const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    listSizes_t& unstrPropertyListSizes, ListHeaders& unstrPropertyListHeaders,
    ListsMetadata& unstrPropertyListsMetadata, InMemUnstrPropertyPages& unstrPropertyPages,
    InMemStringOverflowPages& stringOverflowPages, PageCursor& stringOvfPagesCursor) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
        *unstrPropertyStringBreaker1 = 0;
        auto propertyKeyId = unstrPropertiesNameToIdMap.at(string(unstrPropertyString));
        auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
        *unstrPropertyStringBreaker2 = 0;
        auto dataType = TypeUtils::getDataType(string(unstrPropertyStringBreaker1 + 1));
        auto dataTypeSize = TypeUtils::getDataTypeSize(dataType);
        auto reversePos = ListsLoaderHelper::decrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        PageCursor pageCursor;
        ListsLoaderHelper::calculatePageCursor(unstrPropertyListHeaders.headers[nodeOffset],
            reversePos, 1, nodeOffset, pageCursor, unstrPropertyListsMetadata);
        switch (dataType) {
        case INT64: {
            auto intVal = TypeUtils::convertToInt64(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyId,
                static_cast<uint8_t>(dataType), dataTypeSize, reinterpret_cast<uint8_t*>(&intVal));
            break;
        }
        case DOUBLE: {
            auto doubleVal = TypeUtils::convertToDouble(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyId,
                static_cast<uint8_t>(dataType), dataTypeSize,
                reinterpret_cast<uint8_t*>(&doubleVal));
            break;
        }
        case BOOL: {
            auto boolVal = TypeUtils::convertToBoolean(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyId,
                static_cast<uint8_t>(dataType), dataTypeSize, reinterpret_cast<uint8_t*>(&boolVal));
            break;
        }
        case DATE: {
            char* beginningOfDateStr = unstrPropertyStringBreaker2 + 1;
            date_t dateVal = Date::FromCString(beginningOfDateStr, strlen(beginningOfDateStr));
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyId,
                static_cast<uint8_t>(dataType), dataTypeSize, reinterpret_cast<uint8_t*>(&dateVal));
            break;
        }
        case STRING: {
            auto strVal =
                reader.skipTokenIfNull() ? &gf_string_t::EMPTY_STRING : reader.getString();
            auto encodedString = reinterpret_cast<gf_string_t*>(
                unstrPropertyPages.getPtrToMemLoc(pageCursor) +
                UnstructuredPropertyLists::
                    UNSTR_PROP_HEADER_LEN /*leave space for id and dataType*/);
            stringOverflowPages.setStrInOvfPageAndPtrInEncString(
                strVal, stringOvfPagesCursor, encodedString);
            // in case of string, we want to set only the property key id and datatype.
            unstrPropertyPages.setUnstrProperty(
                pageCursor, propertyKeyId, static_cast<uint8_t>(dataType), 0, nullptr);
            break;
        }
        default:
            throw invalid_argument("unsupported dataType while parsing unstructured property");
        }
    }
}

} // namespace loader
} // namespace graphflow
