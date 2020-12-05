#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/adj_lists_loader_helper.h"
#include "src/loader/include/adj_rels_loader_helper.h"
#include "src/loader/include/csv_reader.h"
#include "src/loader/include/thread_pool.h"
#include "src/storage/include/structures/lists.h"

namespace graphflow {
namespace loader {

// List Sizes.
typedef vector<atomic<uint64_t>> listSizes_t;
typedef vector<vector<unique_ptr<listSizes_t>>> dirLabelListSizes_t;

class RelsLoader {
    friend class GraphLoader;

private:
    RelsLoader(ThreadPool& threadPool, const Graph& graph, const Catalog& catalog,
        const nlohmann::json& metadata, vector<shared_ptr<NodeIDMap>>& nodeIDMaps,
        const string outputDirectory)
        : logger{spdlog::stdout_logger_mt("RelsLoader")}, threadPool{threadPool}, graph{graph},
          catalog{catalog}, metadata{metadata}, nodeIDMaps{nodeIDMaps},
          outputDirectory(outputDirectory){};

    void load(vector<string>& fnames, vector<uint64_t>& numBlocksPerLabel);

    void loadRelsForLabel(RelLabelDescription& relLabelMetadata);

    // constructs AdjRels and PropertyColumns

    void constructAdjRelsAndCountRelsInAdjLists(
        RelLabelDescription& relLabelMetadata, dirLabelListSizes_t& dirLabelListSizes);

    // constructs AdjLists and PropertyLists

    void constructAdjLists(
        RelLabelDescription& description, dirLabelListSizes_t& dirLabelListSizes);

    void initAdjListHeaders(RelLabelDescription& description,
        dirLabelListSizes_t& dirLabelListSizes, AdjListsLoaderHelper& adjListsLoaderHelper);

    void initAdjListsAndPropertyListsMetadata(RelLabelDescription& description,
        dirLabelListSizes_t& dirLabelListSizes, AdjListsLoaderHelper& adjListsLoaderHelper);

    // Concurrent Tasks

    static void populateAdjRelsAndCountRelsInAdjListsTask(RelLabelDescription* description,
        uint64_t blockId, const char tokenSeparator, dirLabelListSizes_t* dirLabelListSizes,
        AdjRelsLoaderHelper* adjRelsLoaderHelper, vector<shared_ptr<NodeIDMap>>* nodeIDMaps,
        const Catalog* catalog, shared_ptr<spdlog::logger> logger);

    static void calculateAdjListHeadersForIndexTask(Direction dir, label_t nodeLabel,
        node_offset_t numNodeOffsets, uint32_t numPerPage, listSizes_t* listSizes,
        AdjListHeaders* adjListHeaders);

    static void calculateListsMetadataForListsTask(uint64_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, AdjListHeaders* adjListHeaders, ListsMetadata* adjListsMetadata);

    static void populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
        const char tokenSeparator, dirLabelListSizes_t* dirLabelListSizes,
        AdjListsLoaderHelper* adjListsLoaderHelper, vector<shared_ptr<NodeIDMap>>* nodeIDMaps,
        const Catalog* catalog, shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static void putPropsOfLineIntoInMemPropertyColumns(const vector<Property>* propertyMap,
        CSVReader& reader, AdjRelsLoaderHelper* adjRelsLoaderHelper, label_t nodeLabel,
        node_offset_t nodeOffset);

    static void calcPageIDAndCSROffsetInPage(uint32_t header, uint64_t pos,
        uint64_t numElementsInAPage, node_offset_t nodeOffset, uint64_t& pageID,
        uint64_t& csrOffsetInPage, ListsMetadata& metadata);

    static void putPropsOfLineIntoInMemRelPropLists(const vector<Property>* propertyMap,
        CSVReader& reader, vector<label_t>& labels, vector<node_offset_t>& offsets,
        vector<uint64_t>& pos, vector<uint32_t>& headers,
        AdjListsLoaderHelper* adjListsLoaderHelper);

    static void setValInAnInMemRelPropLists(uint32_t header, uint64_t pos, uint8_t elementSize,
        node_offset_t& offset, uint8_t* val, ListsMetadata& listsMetadata,
        InMemPropertyLists& propertyLists);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Graph& graph;
    const Catalog& catalog;
    const nlohmann::json& metadata;
    vector<shared_ptr<NodeIDMap>>& nodeIDMaps;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
