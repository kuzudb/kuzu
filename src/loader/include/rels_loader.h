#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/adj_and_property_columns_loader_helper.h"
#include "src/loader/include/adj_and_property_lists_loader_helper.h"
#include "src/loader/include/csv_reader.h"
#include "src/loader/include/thread_pool.h"

namespace graphflow {
namespace loader {

class RelsLoader {
    friend class GraphLoader;

private:
    RelsLoader(ThreadPool& threadPool, const Graph& graph, const Catalog& catalog,
        const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps,
        const string outputDirectory)
        : logger{spdlog::stdout_logger_mt("RelsLoader")}, threadPool{threadPool}, graph{graph},
          catalog{catalog}, metadata{metadata}, nodeIDMaps{nodeIDMaps},
          outputDirectory(outputDirectory){};

    void load(vector<string>& fnames, vector<uint64_t>& numBlocksPerLabel);

    void loadRelsForLabel(RelLabelDescription& relLabelMetadata);

    // constructs AdjRels and PropertyColumns

    void constructAdjRelsAndCountRelsInAdjLists(RelLabelDescription& relLabelMetadata,
        AdjAndPropertyListsLoaderHelper& adjAndPropertyListsLoaderHelper);

    // constructs AdjLists Headers, Lists Metadata, AdjLists and PropertyLists

    void constructAdjLists(RelLabelDescription& description,
        AdjAndPropertyListsLoaderHelper& adjAndPropertyListsLoaderHelper);

    // Concurrent Tasks

    static void populateAdjRelsAndCountRelsInAdjListsTask(RelLabelDescription* description,
        uint64_t blockId, const char tokenSeparator,
        AdjAndPropertyListsLoaderHelper* adjAndPropertyListsLoaderHelper,
        AdjAndPropertyColumnsLoaderHelper* adjAndPropertyColumnsLoaderHelper,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger> logger);

    static void populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
        const char tokenSeparator, AdjAndPropertyListsLoaderHelper* adjAndPropertyListsLoaderHelper,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static void inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        vector<bool>& requireToReadLabels);

    static void putPropsOfLineIntoInMemPropertyColumns(const vector<Property>* propertyMap,
        CSVReader& reader, AdjAndPropertyColumnsLoaderHelper* adjAndPropertyColumnsLoaderHelper,
        const nodeID_t& nodeID, vector<PageCursor>& stringOvreflowPagesCursors,
        shared_ptr<spdlog::logger> logger);

    static void putPropsOfLineIntoInMemRelPropLists(const vector<Property>* propertyMap,
        CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& pos,
        AdjAndPropertyListsLoaderHelper* adjAndPropertyListsLoaderHelper,
        vector<PageCursor>& stringOvreflowPagesCursors, shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Graph& graph;
    const Catalog& catalog;
    const nlohmann::json& metadata;
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
