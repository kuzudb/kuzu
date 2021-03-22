#include "src/loader/include/nodes_loader.h"

#include "src/storage/include/stores/nodes_store.h"

namespace graphflow {
namespace loader {

void NodesLoader::load(const vector<string>& fnames, const vector<uint64_t>& numBlocksPerLabel,
    const vector<vector<uint64_t>>& numLinesPerBlock, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    for (label_t nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        loadNodesForLabel(nodeLabel, numBlocksPerLabel[nodeLabel], fnames[nodeLabel],
            numLinesPerBlock[nodeLabel], *nodeIDMaps[nodeLabel]);
    }
}

void NodesLoader::loadNodesForLabel(const label_t& nodeLabel, const uint64_t& numBlocks,
    const string& fname, const vector<uint64_t>& numLinesPerBlock, NodeIDMap& nodeIDMap) {
    logger->debug("Processing nodeLabel {}.", nodeLabel);
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
    auto propertyDataTypes = createPropertyDataTypes(propertyMap);
    logger->debug("Populating Node PropertyColumns.");
    node_offset_t offsetStart = 0;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        threadPool.execute(populateNodePropertyColumnTask, fname, blockIdx,
            metadata.at("tokenSeparator").get<string>()[0], &propertyDataTypes,
            numLinesPerBlock[blockIdx], offsetStart, &nodeIDMap, &propertyColumnFnames,
            &propertyIdxStringOverflowPages, logger);
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

void NodesLoader::populateNodePropertyColumnTask(string fname, uint64_t blockId,
    char tokenSeparator, const vector<DataType>* propertyDataTypes, uint64_t numElements,
    node_offset_t offsetStart, NodeIDMap* nodeIDMap, vector<string>* propertyColumnFnames,
    vector<unique_ptr<InMemStringOverflowPages>>* propertyIdxStringOverflowPages,
    shared_ptr<spdlog::logger> logger) {
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
        bufferOffset++;
    }
    writeBuffersToFiles(
        *buffers, offsetStart, numElements, *propertyColumnFnames, *propertyDataTypes);
    logger->trace("End: path={0} blkIdx={1}", fname, blockId);
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
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch (propertyDataTypes[propertyIdx]) {
        case INT32: {
            auto intVal = reader.skipTokenIfNull() ? NULL_INT : reader.getInteger();
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
        propertyIdx++;
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