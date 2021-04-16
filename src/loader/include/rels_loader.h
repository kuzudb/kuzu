#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/adj_and_prop_cols_builder_and_list_size_counter.h"
#include "src/loader/include/adj_and_prop_lists_builder.h"
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
        : logger{spdlog::get("loader")}, threadPool{threadPool}, graph{graph}, catalog{catalog},
          metadata{metadata}, nodeIDMaps{nodeIDMaps}, outputDirectory(outputDirectory){};

    void load(vector<string>& fnames, vector<uint64_t>& numBlocksPerLabel);

    void loadRelsForLabel(RelLabelDescription& relLabelMetadata);

    void constructAdjColumnsAndCountRelsInAdjLists(RelLabelDescription& relLabelMetadata,
        AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder);

    void constructAdjLists(
        RelLabelDescription& description, AdjAndPropertyListsBuilder& adjAndPropertyListsBuilder);

    // Concurrent Tasks

    static void populateAdjColumnsAndCountRelsInAdjListsTask(RelLabelDescription* description,
        uint64_t blockId, const char tokenSeparator,
        AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
        AdjAndPropertyColsBuilderAndListSizeCounter* adjAndPropertyColumnsBuilder,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger> logger);

    static void populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
        const char tokenSeparator, AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static void inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        vector<bool>& requireToReadLabels);

    static void putPropsOfLineIntoInMemPropertyColumns(const vector<DataType>& propertyDataTypes,
        CSVReader& reader,
        AdjAndPropertyColsBuilderAndListSizeCounter* adjAndPropertyColumnsBuilder,
        const nodeID_t& nodeID, vector<PageCursor>& stringOvreflowPagesCursors,
        shared_ptr<spdlog::logger> logger);

    static void putPropsOfLineIntoInMemRelPropLists(const vector<DataType>& propertyDataTypes,
        CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& pos,
        AdjAndPropertyListsBuilder* adjAndPropertyListsBuilder,
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
