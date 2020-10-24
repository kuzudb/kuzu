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

    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool{thread::hardware_concurrency()};
    const string inputDirectory;
    const string outputDirectory;

public:
    GraphLoader(string inputDirectory, string outputDirectory);
    ~GraphLoader() = default;
    void loadGraph();

private:
    unique_ptr<nlohmann::json> readMetadata();
    void setNodeAndRelLabels(const nlohmann::json &metadata, Catalog &catalog);
    void assignLabels(
        unordered_map<string, gfLabel_t> &stringToLabelMap, const nlohmann::json &fileDescriptions);
    void setCardinalities(const nlohmann::json &metadata, Catalog &catalog);
    void setSrcDstNodeLabelsForRelLabels(const nlohmann::json &metadata, Catalog &catalog);
    void loadNodes(
        const nlohmann::json &metadata, Catalog &catalog, vector<uint64_t> &numNodesPerLabel);
    void readNodePropertiesAndSerialize(Catalog &catalog, const nlohmann::json &metadata,
        vector<string> &filenames, vector<vector<Property>> &propertyMap,
        vector<uint64_t> &numNodesPerLabel, vector<uint64_t> &numBlocksPerLabel,
        vector<vector<uint64_t>> &numLinesPerBlock);
    unique_ptr<vector<unique_ptr<InMemoryColumnBase>>> createInMemoryColumnsForNodeProperties(
        vector<Property> &propertyMap, Catalog &catalog, uint64_t numNodes);
    void loadRels(
        const nlohmann::json &metadata, Catalog &catalog, vector<uint64_t> &numRelsPerLabel);
    void initPropertyMapAndCalcNumBlocksPerLabel(gfLabel_t numLabels, vector<string> &filenames,
        vector<uint64_t> &numPerLabel, vector<vector<Property>> &propertyMap,
        const nlohmann::json &metadata);
    void parseHeader(const nlohmann::json &metadata, string &header, vector<Property> &propertyMap);
    void countLinesPerBlockAndInitNumPerLabel(gfLabel_t numLabels,
        vector<vector<uint64_t>> &numLinesPerBlock, vector<uint64_t> &numBlocksPerLabel,
        vector<uint64_t> &numPerLabel, const nlohmann::json &metadata, vector<string> &filenames);
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_GRAPH_LOADER_H_
