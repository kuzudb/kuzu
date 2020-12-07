#pragma once

#include <thread>
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
        : logger{spdlog::stdout_logger_mt("GraphLoader")}, threadPool{ThreadPool(numThreads)},
          inputDirectory(inputDirectory), outputDirectory(outputDirectory) {}

    void loadGraph();

private:
    unique_ptr<nlohmann::json> readMetadata();

    void assignLabels(stringToLabelMap_t& map, const nlohmann::json& fileDescriptions);
    void setCardinalities(Catalog& catalog, const nlohmann::json& metadata);
    void setSrcDstNodeLabelsForRelLabels(Catalog& catalog, const nlohmann::json& metadata);

    unique_ptr<vector<shared_ptr<NodeIDMap>>> loadNodes(
        const nlohmann::json& metadata, Graph& graph, Catalog& catalog);

    void loadRels(const nlohmann::json& metadata, Graph& graph, Catalog& catalog,
        unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps);

    void inferFilenamesInitPropertyMapAndCountLinesPerBlock(label_t numLabels,
        nlohmann::json filedescriptions, vector<string>& fnames,
        vector<uint64_t>& numBlocksPerLabel, vector<vector<Property>>& propertyMap,
        const char tokenSeparator);

    void initPropertyMapAndCalcNumBlocksPerLabel(label_t numLabels, vector<string>& fnames,
        vector<uint64_t>& numPerLabel, vector<vector<Property>>& propertyMaps,
        const char tokenSeparator);

    void parseHeader(const char tokenSeparator, string& header, vector<Property>& propertyMap);

    void countLinesPerBlockAndInitNumPerLabel(label_t numLabels,
        vector<vector<uint64_t>>& numLinesPerBlock, vector<uint64_t>& numBlocksPerLabel,
        const char tokenSeparator, vector<string>& fnames, vector<uint64_t>& numPerLabel);

    // Concurrent Tasks
    static void fileBlockLinesCounterTask(string fname, char tokenSeparator,
        vector<vector<uint64_t>>* numLinesPerBlock, label_t label, uint32_t blockId,
        shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool threadPool;
    const string inputDirectory;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
