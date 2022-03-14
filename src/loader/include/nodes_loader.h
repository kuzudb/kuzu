#pragma once

#include "nlohmann/json.hpp"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_node_prop_cols_builder.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/loader/include/loader_progress_bar.h"
#include "src/loader/include/loader_task.h"
#include "src/storage/include/graph.h"

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
        vector<NodeFileDescription>& nodeFileDescriptions, LoaderProgressBar* progressBar);

    void load(const vector<string>& filePaths, const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps);

    void constructPropertyColumnsAndCountUnstrProperties(NodeLabelDescription& description,
        const vector<uint64_t>& numLinesPerBlock, InMemNodePropertyColumnsBuilder& builder);

    void constructUnstrPropertyLists(const vector<string>& filePaths,
        const vector<uint64_t>& numBlocksPerLabel,
        const vector<vector<uint64_t>>& numLinesPerBlock);

    void buildUnstrPropertyListsHeadersAndMetadata();

    void buildInMemUnstrPropertyLists();

    void saveUnstrPropertyListsToFile();

    // Concurrent Tasks

    static void populatePropertyColumnsAndCountUnstrPropertyListSizesTask(
        NodeLabelDescription* description, uint64_t blockId, node_offset_t offsetStart,
        NodeIDMap* nodeIDMap, InMemNodePropertyColumnsBuilder* builder,
        listSizes_t* unstrPropertyListSizes, shared_ptr<spdlog::logger>& logger,
        LoaderProgressBar* progressBar);

    static void populateUnstrPropertyListsTask(const string& fName, uint64_t blockId,
        char tokenSeparator, char quoteChar, char escapeChar, uint32_t numStructuredProperties,
        node_offset_t offsetStart,
        const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        listSizes_t* unstrPropertyListSizes, ListHeaders* unstrPropertyListHeaders,
        ListsMetadata* unstrPropertyListsMetadata, InMemUnstrPropertyPages* unstrPropertyPages,
        InMemStringOverflowPages* stringOverflowPages, shared_ptr<spdlog::logger>& logger,
        LoaderProgressBar* progressBar);

    // Task Helpers

    static void putPropsOfLineIntoInMemPropertyColumns(const vector<PropertyDefinition>& properties,
        CSVReader& reader, InMemNodePropertyColumnsBuilder& builder,
        vector<PageByteCursor>& stringOverflowPagesCursors, NodeIDMap* nodeIDMap,
        uint64_t nodeOffset);

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, listSizes_t& unstrPropertyListSizes);

    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        const unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        listSizes_t& unstrPropertyListSizes, ListHeaders& unstrPropertyListHeaders,
        ListsMetadata& unstrPropertyListsMetadata, InMemUnstrPropertyPages& unstrPropertyPages,
        InMemStringOverflowPages& stringOverflowPages, PageByteCursor& stringOvfPagesCursor);

private:
    shared_ptr<spdlog::logger> logger;
    TaskScheduler& taskScheduler;

    const Graph& graph;
    const string outputDirectory;
    vector<NodeFileDescription>& fileDescriptions;

    // For unstructured Property Lists per node label.
    labelUnstrPropertyListSizes_t labelUnstrPropertyListsSizes{};
    vector<ListHeaders> labelUnstrPropertyListHeaders;
    vector<ListsMetadata> labelUnstrPropertyListsMetadata;
    vector<unique_ptr<InMemUnstrPropertyPages>> labelUnstrPropertyLists;
    vector<unique_ptr<InMemStringOverflowPages>> labelUnstrPropertyListsStringOverflowPages;
    LoaderProgressBar* progressBar;
};

} // namespace loader
} // namespace graphflow
