#pragma once

#include "nlohmann/json.hpp"

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_adj_prop_cols_builder.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_adj_prop_lists_builder.h"
#include "src/loader/include/loader_task.h"

namespace graphflow {
namespace loader {

class RelsLoader {
    friend class GraphLoader;

private:
    RelsLoader(TaskScheduler& taskScheduler, Graph& graph, string outputDirectory,
        vector<unique_ptr<NodeIDMap>>& nodeIDMaps,
        const vector<RelFileDescription>& fileDescriptions, LoaderProgressBar* progressBar);

    void load(vector<uint64_t>& numBlocksPerLabel);

    void loadRelsForLabel(RelLabelDescription& relLabelMetadata);

    void constructAdjColumnsAndCountRelsInAdjLists(
        RelLabelDescription& relLabelMetadata, InMemAdjAndPropertyListsBuilder& listsBuilder);

    void populateNumRels(InMemAdjAndPropertyColumnsBuilder& columnsBuilder,
        InMemAdjAndPropertyListsBuilder& listsBuilder);

    void constructAdjLists(
        RelLabelDescription& description, InMemAdjAndPropertyListsBuilder& listsBuilder);

    // Concurrent Tasks

    static void populateAdjColumnsAndCountRelsInAdjListsTask(RelLabelDescription* description,
        uint64_t blockId, InMemAdjAndPropertyListsBuilder* listsBuilder,
        InMemAdjAndPropertyColumnsBuilder* columnsBuilder,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progresBar);

    static void populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
        const CSVReaderConfig& csvReaderConfig, InMemAdjAndPropertyListsBuilder* listsBuilder,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar);

    // Task Helpers

    static void inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<unique_ptr<NodeIDMap>>* nodeIDMaps, const Catalog* catalog,
        vector<bool>& requireToReadLabels);

    static void putPropsOfLineIntoInMemPropertyColumns(const vector<PropertyDefinition>& properties,
        CSVReader& reader, InMemAdjAndPropertyColumnsBuilder* columnsBuilder,
        const nodeID_t& nodeID, vector<PageByteCursor>& stringOverflowPagesCursors);

    static void putPropsOfLineIntoInMemRelPropLists(const vector<PropertyDefinition>& properties,
        CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& pos,
        InMemAdjAndPropertyListsBuilder* listsBuilder,
        vector<PageByteCursor>& overflowPagesCursors);

private:
    shared_ptr<spdlog::logger> logger;
    TaskScheduler& taskScheduler;
    Graph& graph;
    const string outputDirectory;
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps;
    const vector<RelFileDescription>& fileDescriptions;
    LoaderProgressBar* progressBar;
};

} // namespace loader
} // namespace graphflow
