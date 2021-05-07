#include "src/loader/include/nodes_loader.h"

#include "src/storage/include/store/nodes_store.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

void NodesLoader::load(const vector<string>& fnames, const vector<uint64_t>& numBlocksPerLabel,
    const vector<vector<uint64_t>>& numLinesPerBlock, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    labelUnstrPropertyListsSizes.resize(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
            labelUnstrPropertyListsSizes[nodeLabel] =
                make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    constructPropertyColumnsAndCountUnstrProperties(
        fnames, numBlocksPerLabel, numLinesPerBlock, nodeIDMaps);
    constructUnstrPropertyLists(fnames, numBlocksPerLabel, numLinesPerBlock);
}

// Constructs PropertyColumn for structured properties of all node labels and also counts the number
// of unstructured property and their sizes for each node. The task is executed in parallel by
// calling populatePropertyColumnsAndCountUnstrPropertyListSizesTask(...) on each block of all
// labels.
void NodesLoader::constructPropertyColumnsAndCountUnstrProperties(const vector<string>& fnames,
    const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock,
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    vector<vector<unique_ptr<InMemStringOverflowPages>>> labelPropertyIdxStringOverflowPages(
        graph.getCatalog().getNodeLabelsCount());
    vector<vector<string>> labelPropertyColumnFnames(graph.getCatalog().getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = graph.getCatalog().getPropertyKeyMapForNodeLabel(nodeLabel);
        labelPropertyColumnFnames[nodeLabel].resize(propertyMap.size());
        labelPropertyIdxStringOverflowPages[nodeLabel].resize(propertyMap.size());
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            labelPropertyColumnFnames[nodeLabel][property->second.idx] =
                NodesStore::getNodePropertyColumnFname(outputDirectory, nodeLabel, property->first);
            if (STRING == property->second.dataType) {
                labelPropertyIdxStringOverflowPages[nodeLabel][property->second.idx] =
                    make_unique<InMemStringOverflowPages>(
                        labelPropertyColumnFnames[nodeLabel][property->second.idx] + ".ovf");
            }
        }
    }
    logger->debug("Populating PropertyColumns and Counting unstructured properties.");
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = graph.getCatalog().getPropertyKeyMapForNodeLabel(nodeLabel);
        auto propertyDataTypes = createPropertyDataTypesArray(propertyMap);
        node_offset_t offsetStart = 0;
        for (auto blockIdx = 0u; blockIdx < numBlocksPerLabel[nodeLabel]; blockIdx++) {
            threadPool.execute(populatePropertyColumnsAndCountUnstrPropertyListSizesTask,
                fnames[nodeLabel], blockIdx, metadata.at("tokenSeparator").get<string>()[0],
                propertyDataTypes, numLinesPerBlock[nodeLabel][blockIdx], offsetStart,
                nodeIDMaps[nodeLabel].get(), labelPropertyColumnFnames[nodeLabel],
                &labelPropertyIdxStringOverflowPages[nodeLabel],
                labelUnstrPropertyListsSizes[nodeLabel].get(), logger);
            offsetStart += numLinesPerBlock[nodeLabel][blockIdx];
        }
    }
    threadPool.wait();
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        auto& propertyMap = graph.getCatalog().getPropertyKeyMapForNodeLabel(nodeLabel);
        for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
            if (STRING == property->second.dataType) {
                threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                    labelPropertyIdxStringOverflowPages[nodeLabel][property->second.idx].get());
            }
        }
    }
    threadPool.wait();
    logger->debug("Done populating PropertyColumns and Counting unstructured properties.");
}

// Constructs unstructered PropertyLists for the labels that have atleast one unstructured property.
// The task is executed in parallel by calling populateUnstrPropertyListsTask(...) on each block of
// all labels.
void NodesLoader::constructUnstrPropertyLists(const vector<string>& fnames,
    const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock) {
    buildUnstrPropertyListsHeadersAndMetadata();
    buildInMemUnstrPropertyLists();
    logger->debug("Populating Unstructured Property Lists.");
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
            node_offset_t offsetStart = 0;
            auto listSizes = labelUnstrPropertyListsSizes[nodeLabel].get();
            auto& listsHeaders = labelUnstrPropertyListHeaders[nodeLabel];
            auto& listsMetadata = labelUnstrPropertyListsMetadata[nodeLabel];
            auto unstrPropertyLists = labelUnstrPropertyLists[nodeLabel].get();
            for (auto blockIdx = 0u; blockIdx < numBlocksPerLabel[nodeLabel]; blockIdx++) {
                threadPool.execute(populateUnstrPropertyListsTask, fnames[nodeLabel], blockIdx,
                    metadata.at("tokenSeparator").get<string>()[0],
                    graph.getCatalog().getPropertyKeyMapForNodeLabel(nodeLabel).size(),
                    numLinesPerBlock[nodeLabel][blockIdx], offsetStart,
                    graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel), listSizes,
                    &listsHeaders, &listsMetadata, unstrPropertyLists,
                    labelUnstrPropertyListsStringOverflowPages[nodeLabel].get(), logger);
                offsetStart += numLinesPerBlock[nodeLabel][blockIdx];
            }
        }
    }
    threadPool.wait();
    saveUnstrPropertyListsToFile();
    logger->debug("Done populating Unstructured Property Lists.");
}

// Initializes ListHeaders and ListsMetadata auxilliary structures for all unstructured
// PropertyLists.
void NodesLoader::buildUnstrPropertyListsHeadersAndMetadata() {
    labelUnstrPropertyListHeaders.resize(graph.getCatalog().getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
            labelUnstrPropertyListHeaders[nodeLabel].init(graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    labelUnstrPropertyListsMetadata.resize(graph.getCatalog().getNodeLabelsCount());
    logger->debug("Initializing UnstructuredPropertyListHeaders.");
    for (auto nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); ++nodeLabel) {
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
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
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
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
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
            auto unstrPropertyListsFname =
                NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            labelUnstrPropertyLists[nodeLabel] = make_unique<InMemUnstrPropertyPages>(
                unstrPropertyListsFname, labelUnstrPropertyListsMetadata[nodeLabel].numPages);
            labelUnstrPropertyListsStringOverflowPages[nodeLabel] =
                make_unique<InMemStringOverflowPages>(unstrPropertyListsFname + ".ovf");
        }
    }
}

void NodesLoader::saveUnstrPropertyListsToFile() {
    for (label_t nodeLabel = 0u; nodeLabel < graph.getCatalog().getNodeLabelsCount(); nodeLabel++) {
        if (graph.getCatalog().getUnstrPropertyKeyMapForNodeLabel(nodeLabel).size() > 0) {
            threadPool.execute([&](InMemUnstrPropertyPages* x) { x->saveToFile(); },
                labelUnstrPropertyLists[nodeLabel].get());
            threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                labelUnstrPropertyListsStringOverflowPages[nodeLabel].get());
            auto fname = NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            threadPool.execute([&](ListsMetadata* x, string fname) { x->saveToDisk(fname); },
                &labelUnstrPropertyListsMetadata[nodeLabel], fname);
            threadPool.execute([&](ListHeaders* x, string fname) { x->saveToDisk(fname); },
                &labelUnstrPropertyListHeaders[nodeLabel], fname);
        }
    }
    threadPool.wait();
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset and call the
// parser that reads structured properties of that line and puts in appropraite buffers. Also infers
// the size of list needed to store unstructured properties of the current node. Finally, dump the
// buffers to approriate locations in files on disk.
void NodesLoader::populatePropertyColumnsAndCountUnstrPropertyListSizesTask(string fname,
    uint64_t blockId, char tokenSeparator, const vector<DataType> propertyDataTypes,
    uint64_t numElements, node_offset_t offsetStart, NodeIDMap* nodeIDMap,
    vector<string> propertyColumnFnames,
    vector<unique_ptr<InMemStringOverflowPages>>* propertyIdxStringOverflowPages,
    listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path={0} blkIdx={1}", fname, blockId);
    vector<PageCursor> stringOverflowPagesCursors{propertyDataTypes.size()};
    auto buffers = createBuffersForPropertyMap(propertyDataTypes, numElements, logger);
    CSVReader reader(fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        reader.hasNextToken();
        nodeIDMap->set(reader.getString(), offsetStart + bufferOffset);
        if (0 < propertyDataTypes.size()) {
            putPropsOfLineIntoBuffers(propertyDataTypes, reader, *buffers, bufferOffset,
                *propertyIdxStringOverflowPages, stringOverflowPagesCursors, logger);
        }
        calcLengthOfUnstrPropertyLists(reader, offsetStart + bufferOffset, *unstrPropertyListSizes);
        bufferOffset++;
    }
    writeBuffersToFiles(
        *buffers, offsetStart, numElements, propertyColumnFnames, propertyDataTypes);
    logger->trace("End: path={0} blkIdx={1}", fname, blockId);
}

// Iterate over each line in a block of CSV file. For each line, infer the node offset, skip
// structured properties and calls the parser that reads unstructured properties of that line and
// puts in unstrPropertyLists.
void NodesLoader::populateUnstrPropertyListsTask(string fname, uint64_t blockId,
    char tokenSeparator, uint32_t numProperties, uint64_t numElements, node_offset_t offsetStart,
    const unordered_map<string, PropertyKey> unstrPropertyMap, listSizes_t* unstrPropertyListSizes,
    ListHeaders* unstrPropertyListHeaders, ListsMetadata* unstrPropertyListsMetadata,
    InMemUnstrPropertyPages* unstrPropertyPages, InMemStringOverflowPages* stringOverflowPages,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path={0} blkIdx={1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    PageCursor stringOvfPagesCursor;
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        reader.hasNextToken();
        for (auto i = 0u; i < numProperties; ++i) {
            reader.hasNextToken();
        }
        putUnstrPropsOfALineToLists(reader, offsetStart + bufferOffset, unstrPropertyMap,
            *unstrPropertyListSizes, *unstrPropertyListHeaders, *unstrPropertyListsMetadata,
            *unstrPropertyPages, *stringOverflowPages, stringOvfPagesCursor);
        bufferOffset++;
    }
    logger->trace("End: path={0} blkIdx={1}", fname, blockId);
}

// Calculates the size of unstructured property lists by iterating over unstructured properties on a
// line and deducing the size needed to store each property based on its dataType.
void NodesLoader::calcLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto startPos = strchr(unstrPropertyString, ':') + 1;
        *strchr(startPos, ':') = 0;
        ListsLoaderHelper::incrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN +
                getDataTypeSize(getDataType(string(startPos))));
    }
}

unique_ptr<vector<unique_ptr<uint8_t[]>>> NodesLoader::createBuffersForPropertyMap(
    const vector<DataType>& propertyDataTypes, uint64_t numElements,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Creating buffers of size {}", numElements);
    auto buffers = make_unique<vector<unique_ptr<uint8_t[]>>>(propertyDataTypes.size());
    for (auto propertyIdx = 0u; propertyIdx < propertyDataTypes.size(); propertyIdx++) {
        (*buffers)[propertyIdx] =
            make_unique<uint8_t[]>(numElements * getDataTypeSize(propertyDataTypes[propertyIdx]));
    };
    return buffers;
}

// Parses the line by converting each property value to the corresponding dataType as given by
// propertyDataTypes array and puts the value into appropriate buffer.
void NodesLoader::putPropsOfLineIntoBuffers(const vector<DataType>& propertyDataTypes,
    CSVReader& reader, vector<unique_ptr<uint8_t[]>>& buffers, const uint32_t& bufferOffset,
    vector<unique_ptr<InMemStringOverflowPages>>& propertyIdxStringOverflowPages,
    vector<PageCursor>& stringOverflowPagesCursors, shared_ptr<spdlog::logger> logger) {
    for (auto propertyIdx = 0u; propertyIdx < propertyDataTypes.size(); ++propertyIdx) {
        reader.hasNextToken();
        switch (propertyDataTypes[propertyIdx]) {
        case INT32: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT32 : reader.getInt32();
            memcpy(buffers[propertyIdx].get() + (bufferOffset * getDataTypeSize(INT32)), &intVal,
                getDataTypeSize(INT32));
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
            memcpy(buffers[propertyIdx].get() + (bufferOffset * getDataTypeSize(DOUBLE)),
                &doubleVal, getDataTypeSize(DOUBLE));
            break;
        }
        case BOOL: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
            memcpy(buffers[propertyIdx].get() + (bufferOffset * getDataTypeSize(BOOL)), &boolVal,
                getDataTypeSize(BOOL));
            break;
        }
        case STRING: {
            auto strVal = reader.skipTokenIfNull() ? &EMPTY_STRING : reader.getString();
            auto encodedString = reinterpret_cast<gf_string_t*>(
                buffers[propertyIdx].get() + (bufferOffset * getDataTypeSize(STRING)));
            propertyIdxStringOverflowPages[propertyIdx]->setStrInOvfPageAndPtrInEncString(
                strVal, stringOverflowPagesCursors[propertyIdx], encodedString);
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
    const vector<string>& propertyColumnFnames, const vector<DataType>& propertyDataTypes) {
    for (auto propertyIdx = 0u; propertyIdx < propertyDataTypes.size(); propertyIdx++) {
        auto fd = open(propertyColumnFnames[propertyIdx].c_str(), O_WRONLY | O_CREAT, 0666);
        if (-1 == fd) {
            invalid_argument("cannot open/create file: " + propertyColumnFnames[propertyIdx]);
        }
        auto offsetInFile = offsetStart * getDataTypeSize(propertyDataTypes[propertyIdx]);
        if (-1 == lseek(fd, offsetInFile, SEEK_SET)) {
            throw invalid_argument("Cannot seek to the required offset in file.");
        }
        auto bytesToWrite = numElementsToWrite * getDataTypeSize(propertyDataTypes[propertyIdx]);
        uint64_t bytesWritten = write(fd, buffers[propertyIdx].get(), bytesToWrite);
        if (bytesWritten != bytesToWrite) {
            throw invalid_argument("Cannot write in file.");
        }
        close(fd);
    }
}

// Parses the line by converting each unstructured property value to the dataType that is specified
// with that property puts the value at appropiate location in unstrPropertyPages.
void NodesLoader::putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
    const unordered_map<string, PropertyKey>& unstrPropertyMap, listSizes_t& unstrPropertyListSizes,
    ListHeaders& unstrPropertyListHeaders, ListsMetadata& unstrPropertyListsMetadata,
    InMemUnstrPropertyPages& unstrPropertyPages, InMemStringOverflowPages& stringOverflowPages,
    PageCursor& stringOvfPagesCursor) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
        *unstrPropertyStringBreaker1 = 0;
        auto propertyKeyIdx = unstrPropertyMap.find(string(unstrPropertyString))->second.idx;
        auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
        *unstrPropertyStringBreaker2 = 0;
        auto dataType = getDataType(string(unstrPropertyStringBreaker1 + 1));
        auto dataTypeSize = getDataTypeSize(dataType);
        auto reversePos = ListsLoaderHelper::decrementListSize(unstrPropertyListSizes, nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        PageCursor pageCursor;
        ListsLoaderHelper::calculatePageCursor(unstrPropertyListHeaders.headers[nodeOffset],
            reversePos, 1, nodeOffset, pageCursor, unstrPropertyListsMetadata);
        switch (dataType) {
        case INT32: {
            auto intVal = convertToInt32(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyIdx,
                static_cast<uint8_t>(dataType), dataTypeSize, reinterpret_cast<uint8_t*>(&intVal));
            break;
        }
        case DOUBLE: {
            auto doubleVal = convertToDouble(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyIdx,
                static_cast<uint8_t>(dataType), dataTypeSize,
                reinterpret_cast<uint8_t*>(&doubleVal));
            break;
        }
        case BOOL: {
            auto boolVal = convertToBoolean(unstrPropertyStringBreaker2 + 1);
            unstrPropertyPages.setUnstrProperty(pageCursor, propertyKeyIdx,
                static_cast<uint8_t>(dataType), dataTypeSize, reinterpret_cast<uint8_t*>(&boolVal));
            break;
        }
        case STRING: {
            auto strVal = reader.skipTokenIfNull() ? &EMPTY_STRING : reader.getString();
            auto encodedString = reinterpret_cast<gf_string_t*>(
                unstrPropertyPages.getPtrToMemLoc(pageCursor) +
                UnstructuredPropertyLists::
                    UNSTR_PROP_HEADER_LEN /*leave space for idx and dataType*/);
            stringOverflowPages.setStrInOvfPageAndPtrInEncString(
                strVal, stringOvfPagesCursor, encodedString);
            // in case of string, we want to set only the property key idx and datatype.
            unstrPropertyPages.setUnstrProperty(
                pageCursor, propertyKeyIdx, static_cast<uint8_t>(dataType), 0, nullptr);
            break;
        }
        default:
            throw invalid_argument("unsupported dataType while parsing unstructured property");
        }
    }
}

} // namespace loader
} // namespace graphflow