#ifndef GRAPHFLOW_COMMON_LOADER_GRAPH_LOADER_H_
#define GRAPHFLOW_COMMON_LOADER_GRAPH_LOADER_H_

#include <thread>
#include <vector>

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

#include "src/common/include/types.h"
#include "src/common/loader/include/thread_pool.h"
#include "src/common/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace std;

namespace graphflow {
namespace common {

class GraphLoader {

public:
    GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads);

    void loadGraph();

private:
    unique_ptr<nlohmann::json> readMetadata();

    unique_ptr<vector<shared_ptr<NodeIDMap>>> loadNodes(
        const nlohmann::json &metadata, Graph &graph, Catalog &catalog);

    void readNodePropertiesAndSerialize(Catalog &catalog, const nlohmann::json &metadata,
        vector<string> &filenames, vector<vector<Property>> &propertyMaps,
        vector<uint64_t> &numNodesPerLabel, vector<uint64_t> &numBlocksPerLabel,
        vector<vector<uint64_t>> &numLinesPerBlock, vector<shared_ptr<NodeIDMap>> &nodeIDMaps);

    shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> createFilesForNodeProperties(
        Catalog &catalog, gfLabel_t label, vector<Property> &propertyMap);

    void loadRels(const nlohmann::json &metadata, Graph &graph, Catalog &catalog,
        unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps);

    void inferFilenamesInitPropertyMapAndCountLinesPerBlock(gfLabel_t numLabels,
        nlohmann::json filedescriptions, vector<string> &filenames,
        vector<uint64_t> &numBlocksPerLabel, vector<vector<Property>> &propertyMap,
        vector<vector<uint64_t>> &numLinesPerBlock, const char tokenSeparator,
        vector<uint64_t> &numPerLabel);

    void initPropertyMapAndCalcNumBlocksPerLabel(gfLabel_t numLabels, vector<string> &filenames,
        vector<uint64_t> &numPerLabel, vector<vector<Property>> &propertyMaps,
        const char tokenSeparator);

    void parseHeader(const char tokenSeparator, string &header, vector<Property> &propertyMap);

    void countLinesPerBlockAndInitNumPerLabel(gfLabel_t numLabels,
        vector<vector<uint64_t>> &numLinesPerBlock, vector<uint64_t> &numBlocksPerLabel,
        const char tokenSeparator, vector<string> &filenames);

    void createSingleCardinalityAdjListsIndexes(Graph &graph, Catalog &catalog,
        const char tokenSeparator, vector<uint64_t> &numBlocksPerLabel,
        vector<vector<uint64_t>> &numLinesPerBlock, vector<string> &filenames,
        vector<shared_ptr<NodeIDMap>> &nodeIDMaps);

    shared_ptr<vector<unique_ptr<InMemColAdjList>>> createInMemColAdjListsIndex(Graph &graph,
        Catalog &catalog, shared_ptr<vector<vector<gfLabel_t>>> nodeLabels, gfLabel_t relLabel,
        Direction direction);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool;
    const string inputDirectory;
    const string outputDirectory;
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_GRAPH_LOADER_H_
