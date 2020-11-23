#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class NodesLoader {
    friend class GraphLoader;

private:
    NodesLoader(ThreadPool& threadPool, const Catalog& catalog, const nlohmann::json& metadata,
        const string outputDirectory)
        : logger{spdlog::stdout_logger_mt("NodesLoader")}, threadPool{threadPool}, catalog{catalog},
          metadata{metadata}, outputDirectory{outputDirectory} {};

    void load(vector<string>& filenames, vector<uint64_t>& numNodesPerLabel,
        vector<uint64_t>& numBlocksPerLabel, vector<vector<uint64_t>>& numLinesPerBlock,
        vector<shared_ptr<NodeIDMap>>& nodeIDMaps);

    shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> createFilesForNodeProperties(
        gfLabel_t label, const vector<Property>& propertyMap);

    // Concurrent Tasks

    static void populateNodePropertyColumnTask(string fname, char tokenSeparator,
        const vector<Property>& propertyMap, uint64_t blockId, uint64_t numElements,
        gfNodeOffset_t beginOffset, shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> pageFiles,
        shared_ptr<NodeIDMap> nodeIDMap, shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static unique_ptr<vector<uint8_t*>> getBuffersForWritingNodeProperties(
        const vector<Property>& propertyMap, uint64_t numElements,
        shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Catalog& catalog;
    const nlohmann::json& metadata;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
