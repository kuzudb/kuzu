#include "src/loader/include/nodes_loader.h"

#include "src/storage/include/stores/nodes_store.h"

namespace graphflow {
namespace loader {

void NodesLoader::load(const vector<string>& fnames, const vector<uint64_t>& numBlocksPerLabel,
    const vector<vector<uint64_t>>& numLinesPerBlock, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    labelUnstrPropertyListsSizes.resize(catalog.getNodeLabelsCount());
    for (label_t nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            labelUnstrPropertyListsSizes[nodeLabel] =
                make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesPerLabel()[nodeLabel]);
        }
        constructPropertyColumnsAndCountUnstrProperties(nodeLabel, numBlocksPerLabel[nodeLabel],
            fnames[nodeLabel], numLinesPerBlock[nodeLabel], *nodeIDMaps[nodeLabel]);
    }
    buildUnstrPropertyListsHeadersAndMetadata();
    buildInMemUnstrPropertyLists();
    for (label_t nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            constructUnstrPropertyLists(nodeLabel, numBlocksPerLabel[nodeLabel], fnames[nodeLabel],
                numLinesPerBlock[nodeLabel]);
        }
    }
    for (label_t nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            labelUnstrPropertyLists[nodeLabel]->saveToFile();
            auto fname = NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            threadPool.execute([&](ListsMetadata& x, string fname) { x.saveToFile(fname); },
                labelUnstrPropertyListsMetadata[nodeLabel], fname);
            threadPool.execute([&](ListHeaders& x, string fname) { x.saveToFile(fname); },
                labelUnstrPropertyListHeaders[nodeLabel], fname);
        }
    }
}

void NodesLoader::constructPropertyColumnsAndCountUnstrProperties(const label_t& nodeLabel,
    const uint64_t& numBlocks, const string& fname, const vector<uint64_t>& numLinesPerBlock,
    NodeIDMap& nodeIDMap) {
    logger->debug("Processing PropertyColumns for nodeLabel {}.", nodeLabel);
    auto& propertyMap = catalog.getPropertyMapForNodeLabel(nodeLabel);
    vector<unique_ptr<InMemStringOverflowPages>> propertyIdxStringOverflowPages{propertyMap.size()};
    vector<string> propertyColumnFnames(propertyMap.size());
    for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
        propertyColumnFnames[property->second.idx] =
            NodesStore::getNodePropertyColumnFname(outputDirectory, nodeLabel, property->first);
        if (STRING == property->second.dataType) {
            propertyIdxStringOverflowPages[property->second.idx] =
                make_unique<InMemStringOverflowPages>(
                    propertyColumnFnames[property->second.idx] + ".ovf");
        }
    }
    auto propertyDataTypes = createPropertyDataTypesArray(propertyMap);
    logger->debug("Populating PropertyColumns and Counting unstructured properties.");
    node_offset_t offsetStart = 0;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        threadPool.execute(populatePropertyColumnsAndCountUnstrPropertyListSizesTask, fname,
            blockIdx, metadata.at("tokenSeparator").get<string>()[0], &propertyDataTypes,
            numLinesPerBlock[blockIdx], offsetStart, &nodeIDMap, &propertyColumnFnames,
            &propertyIdxStringOverflowPages, labelUnstrPropertyListsSizes[nodeLabel].get(), logger);
        offsetStart += numLinesPerBlock[blockIdx];
    }
    threadPool.wait();
    for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
        if (STRING == property->second.dataType) {
            threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                propertyIdxStringOverflowPages[property->second.idx].get());
        }
    }
    threadPool.wait();
    logger->debug("Done.");
}

void NodesLoader::buildUnstrPropertyListsHeadersAndMetadata() {
    labelUnstrPropertyListHeaders.resize(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0; nodeLabel < catalog.getNodeLabelsCount(); ++nodeLabel) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            auto a = catalog.getUnstrPropertyMapForNodeLabel(nodeLabel);
            labelUnstrPropertyListHeaders[nodeLabel].headers.resize(
                graph.getNumNodesPerLabel()[nodeLabel]);
        }
    }
    labelUnstrPropertyListsMetadata.resize(catalog.getNodeLabelsCount());
    logger->debug("Initializing UnstructuredPropertyListHeaders.");
    for (auto nodeLabel = 0; nodeLabel < catalog.getNodeLabelsCount(); ++nodeLabel) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            threadPool.execute(ListsLoaderHelper::calculateListHeadersTask,
                graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel], logger);
        }
    }
    threadPool.wait();
    logger->debug("Done.");
    logger->debug("Initializing UnstructuredPropertyListsMetadata.");
    for (auto nodeLabel = 0; nodeLabel < catalog.getNodeLabelsCount(); ++nodeLabel) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
                graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE,
                labelUnstrPropertyListsSizes[nodeLabel].get(),
                &labelUnstrPropertyListHeaders[nodeLabel],
                &labelUnstrPropertyListsMetadata[nodeLabel], logger);
        }
    }
    threadPool.wait();
    logger->debug("Done.");
}

void NodesLoader::buildInMemUnstrPropertyLists() {
    for (label_t nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        if (catalog.getUnstrPropertyMapForNodeLabel(nodeLabel).size() > 0) {
            auto unstrPropertyListsFname =
                NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
            labelUnstrPropertyLists[nodeLabel] = make_unique<InMemUnstrPropertyPages>(
                unstrPropertyListsFname, labelUnstrPropertyListsMetadata[nodeLabel].numPages);
        }
    }
}

void NodesLoader::constructUnstrPropertyLists(label_t nodeLabel, const uint64_t& numBlocks,
    const string& fname, const vector<uint64_t>& numLinesPerBlock) {
    logger->debug("Processing unstructured Property Lists for nodeLabel {}.", nodeLabel);
    auto fName = NodesStore::getNodeUnstrPropertyListsFname(outputDirectory, nodeLabel);
    node_offset_t offsetStart = 0;
    auto numProperties = catalog.getPropertyMapForNodeLabel(nodeLabel).size();
    auto unstrPropertyMap = catalog.getUnstrPropertyMapForNodeLabel(nodeLabel);
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        threadPool.execute(populateUnstrPropertyListsTask, fname, blockIdx,
            metadata.at("tokenSeparator").get<string>()[0], numProperties,
            numLinesPerBlock[blockIdx], offsetStart, &unstrPropertyMap,
            labelUnstrPropertyListsSizes[nodeLabel].get(),
            &labelUnstrPropertyListHeaders[nodeLabel], &labelUnstrPropertyListsMetadata[nodeLabel],
            labelUnstrPropertyLists[nodeLabel].get(), logger);
        offsetStart += numLinesPerBlock[blockIdx];
    }
    logger->debug("Done.");
}

void NodesLoader::populatePropertyColumnsAndCountUnstrPropertyListSizesTask(string fname,
    uint64_t blockId, char tokenSeparator, const vector<DataType>* propertyDataTypes,
    uint64_t numElements, node_offset_t offsetStart, NodeIDMap* nodeIDMap,
    vector<string>* propertyColumnFnames,
    vector<unique_ptr<InMemStringOverflowPages>>* propertyIdxStringOverflowPages,
    listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path={0} blkIdx={1}", fname, blockId);
    vector<PageCursor> stringOverflowPagesCursors{propertyDataTypes->size()};
    auto buffers = createBuffersForPropertyMap(*propertyDataTypes, numElements, logger);
    CSVReader reader(fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        reader.hasNextToken();
        nodeIDMap->set(reader.getString(), offsetStart + bufferOffset);
        if (0 < propertyDataTypes->size()) {
            putPropsOfLineIntoBuffers(*propertyDataTypes, reader, *buffers, bufferOffset,
                *propertyIdxStringOverflowPages, stringOverflowPagesCursors, logger);
        }
        inferLengthOfUnstrPropertyLists(
            reader, offsetStart + bufferOffset, *unstrPropertyListSizes);
        bufferOffset++;
    }
    writeBuffersToFiles(
        *buffers, offsetStart, numElements, *propertyColumnFnames, *propertyDataTypes);
    logger->trace("End: path={0} blkIdx={1}", fname, blockId);
}

void NodesLoader::populateUnstrPropertyListsTask(string fname, uint64_t blockId,
    char tokenSeparator, uint32_t numProperties, uint64_t numElements, node_offset_t offsetStart,
    unordered_map<string, Property>* unstrPropertyMap, listSizes_t* unstrPropertyListSizes,
    ListHeaders* unstrPropertyListHeaders, ListsMetadata* unstrPropertyListsMetadata,
    InMemUnstrPropertyPages* unstrPropertyPages, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path={0} blkIdx={1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        reader.hasNextToken();
        for (auto i = 0u; i < numProperties; ++i) {
            reader.hasNextToken();
        }
        while (reader.hasNextToken()) {
            auto uPropertyString = reader.getString();
            auto uPropertyStringBreaker1 = strchr(uPropertyString, ':');
            *uPropertyStringBreaker1 = 0;
            auto propertyKey = unstrPropertyMap->find(string(uPropertyString))->second.idx;
            auto uPropertyStringBreaker2 = strchr(uPropertyStringBreaker1 + 1, ':');
            *uPropertyStringBreaker2 = 0;
            auto dataType = getDataType(string(uPropertyStringBreaker1 + 1));
            auto dataTypeSize = getDataTypeSize(dataType);
            auto offset = offsetStart + bufferOffset;
            auto reversePos = ListsLoaderHelper::decrementListSize(
                *unstrPropertyListSizes, offset, 5 + dataTypeSize);
            PageCursor pageCursor;
            ListsLoaderHelper::calculatePageCursor(unstrPropertyListHeaders->headers[offset],
                reversePos, 1, offset, pageCursor, *unstrPropertyListsMetadata);
            switch (dataType) {
            case INT32: {
                auto intVal = convertToInt32(uPropertyStringBreaker2 + 1);
                unstrPropertyPages->set(pageCursor, propertyKey, static_cast<uint8_t>(dataType),
                    dataTypeSize, reinterpret_cast<uint8_t*>(&intVal));
                break;
            }
            case DOUBLE: {
                auto doubleVal = convertToDouble(uPropertyStringBreaker2 + 1);
                unstrPropertyPages->set(pageCursor, propertyKey, static_cast<uint8_t>(dataType),
                    dataTypeSize, reinterpret_cast<uint8_t*>(&doubleVal));
                break;
            }
            case BOOL: {
                auto boolVal = convertToBoolean(uPropertyStringBreaker2 + 1);
                unstrPropertyPages->set(pageCursor, propertyKey, static_cast<uint8_t>(dataType),
                    dataTypeSize, reinterpret_cast<uint8_t*>(&boolVal));
                break;
            }
            default:
                throw invalid_argument("unsupported dataType while parsing unstructured property");
            }
        }
        bufferOffset++;
    }
    logger->trace("End: path={0} blkIdx={1}", fname, blockId);
}

void NodesLoader::inferLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        cout << unstrPropertyString << endl;
        auto startPos = strchr(unstrPropertyString, ':') + 1;
        *strchr(startPos, ':') = 0;
        ListsLoaderHelper::incrementListSize(unstrPropertyListSizes, nodeOffset,
            4 + 1 + getDataTypeSize(getDataType(string(startPos))));
    }
}

unique_ptr<vector<unique_ptr<uint8_t[]>>> NodesLoader::createBuffersForPropertyMap(
    const vector<DataType>& propertyDataTypes, uint64_t numElements,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Creating buffers for {}", numElements);
    auto buffers = make_unique<vector<unique_ptr<uint8_t[]>>>(propertyDataTypes.size());
    for (auto propertyIdx = 0u; propertyIdx < propertyDataTypes.size(); propertyIdx++) {
        (*buffers)[propertyIdx] =
            make_unique<uint8_t[]>(numElements * getDataTypeSize(propertyDataTypes[propertyIdx]));
    };
    return buffers;
}

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
            propertyIdxStringOverflowPages[propertyIdx]->set(
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

} // namespace loader
} // namespace graphflow