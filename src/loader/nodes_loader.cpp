#include "src/loader/include/nodes_loader.h"

#include "src/loader/include/csv_reader.h"
#include "src/storage/include/stores/nodes_store.h"

namespace graphflow {
namespace loader {

void NodesLoader::load(vector<string>& filenames, vector<uint64_t>& numNodesPerLabel,
    vector<uint64_t>& numBlocksPerLabel, vector<vector<uint64_t>>& numLinesPerBlock,
    vector<shared_ptr<NodeIDMap>>& nodeIDMappings) {
    logger->info("Starting to read and store node properties.");
    vector<shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>>> files(
        catalog.getNodeLabelsCount());
    vector<node_offset_t> frontierOffsets(catalog.getNodeLabelsCount());
    for (label_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
        files[label] =
            createFilesForNodeProperties(label, catalog.getPropertyMapForNodeLabel(label));
        frontierOffsets[label] = 0;
    }
    auto maxNumBlocks = *max_element(begin(numBlocksPerLabel), end(numBlocksPerLabel));
    for (auto blockId = 0u; blockId < maxNumBlocks; blockId++) {
        for (label_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
            if (blockId < numBlocksPerLabel[label]) {
                threadPool.execute(populateNodePropertyColumnTask, filenames[label],
                    metadata.at("tokenSeparator").get<string>()[0],
                    catalog.getPropertyMapForNodeLabel(label), blockId,
                    numLinesPerBlock[label][blockId], frontierOffsets[label], files[label],
                    nodeIDMappings[label], logger);
                frontierOffsets[label] += numLinesPerBlock[label][blockId];
            }
        }
    }
    threadPool.wait();
}

shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> NodesLoader::createFilesForNodeProperties(
    label_t label, const vector<Property>& propertyMap) {
    auto files = make_shared<pair<unique_ptr<mutex>, vector<uint32_t>>>();
    files->first = make_unique<mutex>();
    files->second.resize(propertyMap.size());
    for (auto i = 0u; i < propertyMap.size(); i++) {
        if (STRING == propertyMap[i].dataType) {
            logger->warn("Ignoring string properties.");
        } else {
            auto fname =
                NodesStore::getNodePropertyColumnFname(outputDirectory, label, propertyMap[i].name);
            files->second[i] = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
            if (-1u == files->second[i]) {
                invalid_argument("cannot create file: " + fname);
            }
        }
    }
    return files;
}

void NodesLoader::populateNodePropertyColumnTask(string fname, char tokenSeparator,
    const vector<Property>& propertyMap, uint64_t blockId, uint64_t numElements,
    node_offset_t beginOffset, shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> pageFiles,
    shared_ptr<NodeIDMap> nodeIDMap, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    auto buffers = getBuffersForWritingNodeProperties(propertyMap, numElements, logger);
    CSVReader reader(fname, tokenSeparator, blockId);
    NodeIDMap map(numElements);
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        reader.hasNextToken();
        map.setOffset(reader.getString(), beginOffset + bufferOffset);
        auto propertyIdx = 0;
        while (reader.hasNextToken()) {
            switch (propertyMap[propertyIdx].dataType) {
            case INT: {
                auto intVal = reader.skipTokenIfNull() ? NULL_INT : reader.getInteger();
                memcpy((*buffers)[propertyIdx].get() + (bufferOffset * getDataTypeSize(INT)),
                    &intVal, getDataTypeSize(INT));
                break;
            }
            case DOUBLE: {
                auto doubleVal = reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
                memcpy((*buffers)[propertyIdx].get() + (bufferOffset * getDataTypeSize(DOUBLE)),
                    &doubleVal, getDataTypeSize(DOUBLE));
                break;
            }
            case BOOL: {
                auto boolVal = reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
                memcpy((*buffers)[propertyIdx].get() + (bufferOffset * getDataTypeSize(BOOL)),
                    &boolVal, getDataTypeSize(BOOL));
                break;
            }
            default:
                if (!reader.skipTokenIfNull()) {
                    reader.skipToken();
                }
            }
            propertyIdx++;
        }
        bufferOffset++;
    }
    lock_guard lck(*pageFiles->first);
    nodeIDMap->merge(map);
    for (auto i = 0u; i < propertyMap.size(); i++) {
        if (STRING == propertyMap[i].dataType) {
            continue;
        }
        auto offsetInFile = beginOffset * getDataTypeSize(propertyMap[i].dataType);
        if (-1 == lseek(pageFiles->second[i], offsetInFile, SEEK_SET)) {
            throw invalid_argument("Cannot seek to the required offset in file.");
        }
        auto bytesToWrite = numElements * getDataTypeSize(propertyMap[i].dataType);
        uint64_t bytesWritten = write(pageFiles->second[i], (*buffers)[i].get(), bytesToWrite);
        if (bytesWritten != bytesToWrite) {
            throw invalid_argument("Cannot write in file.");
        }
    }
    logger->debug("end   {0} {1}", fname, blockId);
}

unique_ptr<vector<unique_ptr<uint8_t[]>>> NodesLoader::getBuffersForWritingNodeProperties(
    const vector<Property>& propertyMap, uint64_t numElements, shared_ptr<spdlog::logger> logger) {
    logger->debug("creating buffers for elements: {0}", numElements);
    auto buffers = make_unique<vector<unique_ptr<uint8_t[]>>>(propertyMap.size());
    for (auto propertyIdx = 0; propertyIdx < propertyMap.size(); propertyIdx++) {
        auto& property = propertyMap[propertyIdx];
        if (STRING == property.dataType) {
            // TODO: To be implemented later.
        } else {
            (*buffers)[propertyIdx] =
                make_unique<uint8_t[]>(numElements * getDataTypeSize(property.dataType));
        }
    };
    return buffers;
}

} // namespace loader
} // namespace graphflow