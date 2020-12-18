#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/csv_reader.h"
#include "src/loader/include/in_mem_pages.h"
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

    void load(const vector<string>& fnames, const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void loadNodesForLabel(const label_t& nodeLabel, const uint64_t& numBlocks, const string& fname,
        const vector<uint64_t>& numLinesPerBlock, NodeIDMap& nodeIDMap);

    // Concurrent Tasks

    static void populateNodePropertyColumnTask(string fname, uint64_t blockId, char tokenSeparator,
        const vector<Property>* propertyMap, uint64_t numElements, node_offset_t offsetStart,
        NodeIDMap* nodeIDMap, vector<string>* propertyColumnFnames,
        vector<unique_ptr<InMemStringOverflowPages>>* stringOverflowPages,
        shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static unique_ptr<vector<unique_ptr<uint8_t[]>>> createBuffersForPropertyMap(
        const vector<Property>& propertyMap, uint64_t numElements,
        shared_ptr<spdlog::logger> logger);

    static void putPropsOfLineIntoBuffers(const vector<Property>* propertyMap, CSVReader& reader,
        vector<unique_ptr<uint8_t[]>>& buffers, const uint32_t& bufferOffset,
        vector<unique_ptr<InMemStringOverflowPages>>& InMemStringOverflowPages,
        vector<PageCursor>& stringOverflowPagesCursors, shared_ptr<spdlog::logger> logger);

    static void writeBuffersToFiles(const vector<unique_ptr<uint8_t[]>>& buffers,
        const uint64_t& offsetStart, const uint64_t& numElementsToWrite,
        const vector<string>& propertyColumnFnames, const vector<Property>& propertyMap);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Catalog& catalog;
    const nlohmann::json& metadata;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
