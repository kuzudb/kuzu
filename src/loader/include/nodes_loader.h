#pragma once

#include "nlohmann/json.hpp"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/csv_format.h"
#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/loader_task.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace loader {

class NodesLoader {
    friend class GraphLoader;

    // For unstructured Property Lists per node label.
    typedef vector<unique_ptr<listSizes_t>> labelUnstrPropertyListSizes_t;

private:
    NodesLoader(TaskScheduler& taskScheduler, const Graph& graph, string outputDirectory,
        CSVFormat csvFormat);

    void load(const vector<string>& filePaths, const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void constructPropertyColumnsAndCountUnstrProperties(const vector<string>& filePaths,
        const vector<uint64_t>& numBlocksPerLabel, const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void constructUnstrPropertyLists(const vector<string>& filePaths,
        const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock);

    void buildUnstrPropertyListsHeadersAndMetadata();

    void buildInMemUnstrPropertyLists();

    void saveUnstrPropertyListsToFile();

    // Concurrent Tasks

    static void populatePropertyColumnsAndCountUnstrPropertyListSizesTask(const string& fName,
        uint64_t blockId, char tokenSeparator, char quoteChar, char escapeChar,
        const vector<PropertyDefinition>& properties, uint64_t numElements,
        node_offset_t offsetStart, NodeIDMap* nodeIDMap, const vector<string>& propertyColumnFNames,
        vector<unique_ptr<InMemStringOverflowPages>>* stringOverflowPages,
        listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger>& logger);

    static void populateUnstrPropertyListsTask(const string& fName, uint64_t blockId,
        char tokenSeparator, char quoteChar, char escapeChar, uint32_t numStructuredProperties,
        node_offset_t offsetStart,
        const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        listSizes_t* unstrPropertyListSizes, ListHeaders* unstrPropertyListHeaders,
        ListsMetadata* unstrPropertyListsMetadata, InMemUnstrPropertyPages* unstrPropertyPages,
        InMemStringOverflowPages* stringOverflowPages, shared_ptr<spdlog::logger>& logger);

    // Task Helpers

    static unique_ptr<vector<unique_ptr<uint8_t[]>>> createBuffersForProperties(
        const vector<PropertyDefinition>& properties, uint64_t numElements,
        shared_ptr<spdlog::logger>& logger);

    static void putPropsOfLineIntoBuffers(const vector<PropertyDefinition>& properties,
        CSVReader& reader, vector<unique_ptr<uint8_t[]>>& buffers, const uint32_t& bufferOffset,
        vector<unique_ptr<InMemStringOverflowPages>>& inMemStringOverflowPages,
        vector<PageCursor>& stringOverflowPagesCursors, NodeIDMap* nodeIDMap, uint64_t nodeOffset);

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes);

    static void writeBuffersToFiles(const vector<unique_ptr<uint8_t[]>>& buffers,
        const uint64_t& offsetStart, const uint64_t& numElementsToWrite,
        const vector<string>& propertyColumnFnames, const vector<PropertyDefinition>& properties);

    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        listSizes_t& unstrPropertyListSizes, ListHeaders& unstrPropertyListHeaders,
        ListsMetadata& unstrPropertyListsMetadata, InMemUnstrPropertyPages& unstrPropertyPages,
        InMemStringOverflowPages& stringOverflowPages, PageCursor& stringOvfPagesCursor);

private:
    shared_ptr<spdlog::logger> logger;
    TaskScheduler& taskScheduler;

    const Graph& graph;
    const string outputDirectory;

    CSVFormat csvFormat;
    labelUnstrPropertyListSizes_t labelUnstrPropertyListsSizes;
    vector<ListHeaders> labelUnstrPropertyListHeaders;
    vector<ListsMetadata> labelUnstrPropertyListsMetadata;
    vector<unique_ptr<InMemUnstrPropertyPages>> labelUnstrPropertyLists;
    vector<unique_ptr<InMemStringOverflowPages>> labelUnstrPropertyListsStringOverflowPages;
};

} // namespace loader
} // namespace graphflow
