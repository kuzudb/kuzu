#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/csv_reader.h"
#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/lists/lists.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class NodesLoader {
    friend class GraphLoader;

    // For unstructured Property Lists per node label.
    typedef vector<unique_ptr<listSizes_t>> labelUnstrPropertyListSizes_t;
    typedef vector<ListHeaders> labelListHeaders_t;
    typedef vector<ListsMetadata> labelListsMetadata_t;
    typedef vector<unique_ptr<InMemUnstrPropertyPages>> labelUnstrPropertyLists_t;

    typedef vector<unique_ptr<InMemStringOverflowPages>> labelStringOverflowPages_t;

private:
    NodesLoader(ThreadPool& threadPool, const Graph& graph, const nlohmann::json& metadata,
        const string outputDirectory)
        : logger{spdlog::get("loader")}, threadPool{threadPool}, graph{graph}, metadata{metadata},
          outputDirectory{outputDirectory} {
        logger->debug("Initializing NodesLoader.");
    };

    void load(const vector<string>& fnames, const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void constructPropertyColumnsAndCountUnstrProperties(const vector<string>& fnames,
        const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void constructUnstrPropertyLists(const vector<string>& fnames,
        const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock);

    void buildUnstrPropertyListsHeadersAndMetadata();

    void buildInMemUnstrPropertyLists();

    void saveUnstrPropertyListsToFile();

    // Concurrent Tasks

    static void populatePropertyColumnsAndCountUnstrPropertyListSizesTask(string fname,
        uint64_t blockId, char tokenSeparator, const vector<DataType> propertyDataTypes,
        uint64_t numElements, node_offset_t offsetStart, NodeIDMap* nodeIDMap,
        vector<string> propertyColumnFnames,
        vector<unique_ptr<InMemStringOverflowPages>>* stringOverflowPages,
        listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger> logger);

    static void populateUnstrPropertyListsTask(string fname, uint64_t blockId, char tokenSeparator,
        uint32_t numProperties, uint64_t numElements, node_offset_t offsetStart,
        const unordered_map<string, PropertyKey> unstrPropertyMap,
        listSizes_t* unstrPropertyListSizes, ListHeaders* unstrPropertyListHeaders,
        ListsMetadata* unstrPropertyListsMetadata, InMemUnstrPropertyPages* unstrPropertyPages,
        InMemStringOverflowPages* stringOverflowPages, shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static unique_ptr<vector<unique_ptr<uint8_t[]>>> createBuffersForPropertyMap(
        const vector<DataType>& propertyDataTypes, uint64_t numElements,
        shared_ptr<spdlog::logger> logger);

    static void putPropsOfLineIntoBuffers(const vector<DataType>& propertyDataTypes,
        CSVReader& reader, vector<unique_ptr<uint8_t[]>>& buffers, const uint32_t& bufferOffset,
        vector<unique_ptr<InMemStringOverflowPages>>& InMemStringOverflowPages,
        vector<PageCursor>& stringOverflowPagesCursors, shared_ptr<spdlog::logger> logger);

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes);

    static void writeBuffersToFiles(const vector<unique_ptr<uint8_t[]>>& buffers,
        const uint64_t& offsetStart, const uint64_t& numElementsToWrite,
        const vector<string>& propertyColumnFnames, const vector<DataType>& propertyDataTypes);

    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        const unordered_map<string, PropertyKey>& unstrPropertyMap,
        listSizes_t& unstrPropertyListSizes, ListHeaders& unstrPropertyListHeaders,
        ListsMetadata& unstrPropertyListsMetadata, InMemUnstrPropertyPages& unstrPropertyPages,
        InMemStringOverflowPages& stringOverflowPages, PageCursor& stringOvfPagesCursor);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Graph& graph;
    const nlohmann::json& metadata;
    const string outputDirectory;

    labelUnstrPropertyListSizes_t labelUnstrPropertyListsSizes;
    labelListHeaders_t labelUnstrPropertyListHeaders;
    labelListsMetadata_t labelUnstrPropertyListsMetadata;
    labelUnstrPropertyLists_t labelUnstrPropertyLists;
    vector<unique_ptr<InMemStringOverflowPages>> labelUnstrPropertyListsStringOverflowPages;
};

} // namespace loader
} // namespace graphflow
