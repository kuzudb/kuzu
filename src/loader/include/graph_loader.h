#pragma once

#include <thread>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/include/types.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace std;

namespace graphflow {
namespace loader {

class GraphLoader {

public:
    GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads)
        : logger{spdlog::stdout_logger_mt("loader")}, threadPool{ThreadPool(numThreads)},
          inputDirectory(inputDirectory), outputDirectory(outputDirectory) {}
    ~GraphLoader() { spdlog::drop("loader"); };

    void loadGraph();

private:
    unique_ptr<nlohmann::json> readMetadata();

    void assignLabels(stringToLabelMap_t& map, const nlohmann::json& fileDescriptions);
    void setCardinalities(const nlohmann::json& metadata);
    void setSrcDstNodeLabelsForRelLabels(const nlohmann::json& metadata);

    unique_ptr<vector<unique_ptr<NodeIDMap>>> loadNodes(const nlohmann::json& metadata);
    void loadRels(const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void inferFilenamesInitPropertyMapAndCalcNumBlocks(label_t numLabels,
        nlohmann::json filedescriptions, vector<string>& fnames,
        vector<uint64_t>& numBlocksPerLabel, vector<unordered_map<string, Property>>& propertyMaps,
        const char tokenSeparator);

    void parseHeader(
        const char tokenSeparator, string& header, unordered_map<string, Property>& propertyMap);

    void countNodesAndInitUnstrPropertyMaps(vector<vector<uint64_t>>& numLinesPerBlock,
        vector<uint64_t>& numBlocksPerLabel, const char tokenSeparator, vector<string>& fnames);

    void initUnstrPropertyMapForLabel(label_t label,
        vector<unordered_set<const char*, charArrayHasher, charArrayEqualTo>>& unstrPropertyKeys);

    // Concurrent Tasks

    static void countLinesAndScanUnstrPropertiesInBlockTask(string fname, char tokenSeparator,
        const uint32_t numProperties,
        unordered_set<const char*, charArrayHasher, charArrayEqualTo>* unstrPropertyKeys,
        vector<vector<uint64_t>>* numLinesPerBlock, label_t label, uint32_t blockId,
        shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool;
    const string inputDirectory;
    const string outputDirectory;

    Graph graph;
    Catalog catalog;
};

} // namespace loader
} // namespace graphflow
