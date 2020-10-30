#ifndef GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_
#define GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_

#include <mutex>
#include <string>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/common/loader/include/csv_reader.h"
#include "src/common/loader/include/utils.h"

using namespace std;

namespace graphflow {
namespace common {

class LoaderTasks {

public:
    static void fileBlockLinesCounterTask(string fname, char tokenSeparator,
        vector<vector<uint64_t>> *numLinesPerBlock, gfLabel_t label, uint32_t blockId,
        shared_ptr<spdlog::logger> logger);
    static void nodePropertyReaderTask(string fname, char tokenSeparator,
        vector<Property> &propertyMap, uint64_t blockId, uint64_t numElements,
        gfNodeOffset_t offset, shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> files,
        shared_ptr<NodeIDMap> nodeIDMapping, shared_ptr<spdlog::logger> logger);
    static void readColumnAdjListsTask(string fname, uint64_t blockId, const char tokenSeparator,
        vector<bool> isSingleCardinality, shared_ptr<vector<vector<gfLabel_t>>> nodeLabels,
        shared_ptr<vector<shared_ptr<vector<unique_ptr<InMemColAdjList>>>>> inMemColAdjLists,
        shared_ptr<vector<vector<shared_ptr<NodeIDMap>>>> nodeIDMaps,
        shared_ptr<atomic<int>> finishedBlocksCounter, uint64_t numBlocks, bool hasProperties,
        shared_ptr<spdlog::logger> logger);

private:
    static unique_ptr<vector<uint8_t *>> getBuffersForWritingNodeProperties(
        vector<Property> &propertyMap, uint64_t numElements, shared_ptr<spdlog::logger> logger);

    static void resolveNodeIDMapIdxAndOffset(string &nodeID,
        vector<shared_ptr<NodeIDMap>> &nodeIDMaps, vector<gfLabel_t> &nodeLabels, gfLabel_t &label,
        gfNodeOffset_t &offset);
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_
