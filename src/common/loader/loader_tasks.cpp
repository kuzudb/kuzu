#include "src/common/loader/include/loader_tasks.h"

namespace graphflow {
namespace common {

void LoaderTasks::fileBlockLinesCounterTask(string fname, char tokenSeparator,
    vector<vector<uint64_t>> *numLinesPerBlock, gfLabel_t label, uint32_t blockId,
    shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasMore()) {
        (*numLinesPerBlock)[label][blockId]++;
        reader.skipLine();
    }
    logger->debug("end   {0} {1} {2}", fname, blockId, (*numLinesPerBlock)[label][blockId]);
}

void LoaderTasks::nodePropertyReaderTask(string fname, char tokenSeparator,
    vector<Property> &propertyMap, uint64_t blockId, uint64_t numElements, gfNodeOffset_t offset,
    shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> files,
    shared_ptr<NodeIDMap> nodeIDMapping, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    auto buffers = getBuffersForWritingNodeProperties(propertyMap, numElements, logger);
    CSVReader reader(fname, tokenSeparator, blockId);
    NodeIDMap map(numElements);
    if (0 == blockId) {
        reader.skipLine(); // skip header line
    }
    auto bufferOffset = 0u;
    while (reader.hasMore()) {
        map.setOffset(*reader.getNodeID().get(), offset + bufferOffset);
        for (auto i = 1u; i < propertyMap.size(); i++) {
            switch (propertyMap[i].dataType) {
            case INT: {
                auto intVal = reader.skipTokenIfNULL() ? NULL_GFINT : reader.getInteger();
                memcpy((*buffers)[i] + (bufferOffset * getDataTypeSize(INT)), &intVal,
                    getDataTypeSize(INT));
                break;
            }
            case DOUBLE: {
                auto doubleVal = reader.skipTokenIfNULL() ? NULL_GFDOUBLE : reader.getDouble();
                memcpy((*buffers)[i] + (bufferOffset * getDataTypeSize(DOUBLE)), &doubleVal,
                    getDataTypeSize(DOUBLE));
                break;
            }
            case BOOLEAN: {
                auto boolVal = reader.skipTokenIfNULL() ? NULL_GFBOOL : reader.getBoolean();
                memcpy((*buffers)[i] + (bufferOffset * getDataTypeSize(BOOLEAN)), &boolVal,
                    getDataTypeSize(BOOLEAN));
                break;
            }
            default:
                if (!reader.skipTokenIfNULL()) {
                    reader.skipToken();
                }
            }
        }
        bufferOffset++;
    }
    lock_guard lck(*files->first);
    nodeIDMapping->merge(map);
    for (auto i = 1u; i < propertyMap.size(); i++) {
        auto dataType = propertyMap[i].dataType;
        if (STRING == dataType) {
            continue;
        }
        auto offsetInFile = offset * getDataTypeSize(dataType);
        if (-1 == lseek(files->second[i], offsetInFile, SEEK_SET)) {
            throw invalid_argument("Cannot seek to the required offset in file.");
        }
        auto bytesToWrite = numElements * getDataTypeSize(dataType);
        uint64_t bytesWritten = write(files->second[i], (*buffers)[i], bytesToWrite);
        if (bytesWritten != bytesToWrite) {
            throw invalid_argument("Cannot write in file.");
        }
    }
    logger->debug("end   {0} {1}", fname, blockId);
}

unique_ptr<vector<uint8_t *>> LoaderTasks::getBuffersForWritingNodeProperties(
    vector<Property> &propertyMap, uint64_t numElements, shared_ptr<spdlog::logger> logger) {
    logger->debug("creating buffers for elements: {0}", numElements);
    auto buffers = make_unique<vector<uint8_t *>>();
    for (auto &property : propertyMap) {
        if (STRING == property.dataType) {
            (*buffers).push_back(nullptr);
        } else {
            (*buffers).push_back(new uint8_t[numElements * getDataTypeSize(property.dataType)]);
        }
    }
    return buffers;
}

void LoaderTasks::readColumnAdjListsTask(string fname, uint64_t blockId, const char tokenSeparator,
    vector<bool> isSingleCardinality, shared_ptr<vector<vector<gfLabel_t>>> nodeLabels,
    shared_ptr<vector<shared_ptr<vector<unique_ptr<InMemColAdjList>>>>> inMemColAdjLists,
    shared_ptr<vector<vector<shared_ptr<NodeIDMap>>>> nodeIDMaps,
    shared_ptr<atomic<int>> finishedBlocksCounter, uint64_t numBlocks, bool hasProperties,
    shared_ptr<spdlog::logger> logger) {
    logger->info("start {0} {1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    if (0 == blockId) {
        reader.skipLine(); // skip header line
    }
    vector<gfLabel_t> labels(2);
    vector<gfNodeOffset_t> offsets(2);
    while (reader.hasMore()) {
        for (auto direction : DIRECTIONS) {
            resolveNodeIDMapIdxAndOffset(*reader.getNodeID(), (*nodeIDMaps)[direction],
                (*nodeLabels)[direction], labels[direction], offsets[direction]);
        }
        for (auto direction : DIRECTIONS) {
            if (isSingleCardinality[direction]) {
                (*(*inMemColAdjLists)[direction])[labels[direction]]->set(
                    offsets[direction], labels[!direction], offsets[!direction]);
            }
        }
        if (hasProperties) {
            reader.skipLine();
        }
    }
    (*finishedBlocksCounter)++;
    if (finishedBlocksCounter->load() == numBlocks) {
        for (auto direction : DIRECTIONS) {
            if (isSingleCardinality[direction]) {
                for (auto &col : *(*inMemColAdjLists)[direction]) {
                    if (col) {
                        col->saveToFile();
                        col.release();
                    }
                }
            }
        }
    }
    logger->info("end   {0} {1}", fname, blockId);
}

void LoaderTasks::resolveNodeIDMapIdxAndOffset(string &nodeID,
    vector<shared_ptr<NodeIDMap>> &nodeIDMaps, vector<gfLabel_t> &nodeLabels, gfLabel_t &label,
    gfNodeOffset_t &offset) {
    auto i = 0;
    while (!nodeIDMaps[i]->hasNodeID(nodeID)) {
        i++;
    }
    offset = nodeIDMaps[i]->getOffset(nodeID);
    label = nodeLabels[i];
}

} // namespace common
} // namespace graphflow
