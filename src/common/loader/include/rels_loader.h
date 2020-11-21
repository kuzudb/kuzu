#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/loader/include/thread_pool.h"
#include "src/common/loader/include/utils.h"
#include "src/storage/include/adjlists_index.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace std;

// Holds information of a single rel label that is needed to construct adjEdges/adjList
// indexes for that label.
struct RelLabelMetadata {
    gfLabel_t label;
    string fname;
    uint64_t numBlocks;
    vector<vector<gfLabel_t>> nodeLabelsPerDir{2};
    vector<bool> isSingleCardinalityPerDir{false, false};
    vector<pair<uint32_t, uint32_t>> numBytesSchemePerDir{2};
};

// stores the size of adjList of a node offset in an AdjLists index.
typedef vector<atomic<uint64_t>> listSizes_t;

typedef vector<vector<unique_ptr<listSizes_t>>> dirLabelListSizes_t;
typedef vector<vector<AdjListsMetadata>> dirLabelAdjListsMetadata_t;

namespace graphflow {
namespace common {

class RelsLoader {
    friend class GraphLoader;

private:
    RelsLoader(ThreadPool &threadPool, const Graph &graph, const Catalog &catalog,
        const nlohmann::json &metadata, vector<shared_ptr<NodeIDMap>> &nodeIDMaps,
        const string outputDirectory)
        : logger{spdlog::stdout_logger_mt("relsLoader")}, threadPool{threadPool}, graph{graph},
          catalog{catalog}, metadata{metadata}, nodeIDMaps{nodeIDMaps}, outputDirectory{
                                                                            outputDirectory} {};

    void load(vector<string> &fnames, vector<uint64_t> &numBlocksPerLabel);

    void loadRelsForLabel(RelLabelMetadata &relLabelMetadata);

    // adjEdges Indexes

    // constructs adjEdges indexes if the rel label is single cardinality in a direction or
    // otherwise, counts the rels in the adjLists of each node offset.
    void constructAdjEdgesAndCountRelsInAdjLists(
        RelLabelMetadata &relLabelMetadata, dirLabelListSizes_t &dirLabelListSizes);

    // builds in-memory adjEdges indexes for all src node labels (if the rel label is single
    // cardinality in the forward direction) and the dst node labels (if the rel label is single
    // cardinality in the backward direction)
    unique_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> buildInMemAdjEdges(
        RelLabelMetadata &relLabelMetadata);

    // adjLists Indexes

    // constructs adjLists indexes if the rel label is multi cardinality in a direction.
    void contructAdjLists(RelLabelMetadata &relLabelMetadata,
        dirLabelListSizes_t &dirLabelListSizes,
        dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata);

    // Allocate space in adjLists pages to all the adjLists indexes of a particular label.
    void initAdjListsMetadata(RelLabelMetadata &relLabelMetadata,
        dirLabelListSizes_t &dirLabelListSizes,
        dirLabelAdjListsMetadata_t &dirLabelAdjListsIndexMetadata);

    // Populates rels in adjLists indexes using the space allocation scheme of
    // allocateAdjListsPages().
    void populateAdjLists(RelLabelMetadata &relLabelMetadata,
        dirLabelListSizes_t &dirLabelListSizes,
        dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata);
    // builds in-memory adjLists indexes for all src node labels (if the rel label is multi
    // cardinality in the forward direction) and the dst node labels (if the rel label is multi
    // cardinality in the backward direction)
    unique_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>> buildInMemAdjLists(
        RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsMetadata_t &dirLabelAdjListsIndexMetadata);

    // Concurrent Tasks & Helpers

    static void populateAdjEdgesAndCountRelsInAdjListsTask(RelLabelMetadata *relLabelMetadata,
        uint64_t blockId, const char tokenSeparator, dirLabelListSizes_t *dirLabelListSizes,
        vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>> *adjEdgesIndexes,
        vector<shared_ptr<NodeIDMap>> *nodeIDMaps, const Catalog *catalog, bool hasProperties,
        shared_ptr<spdlog::logger> logger);

    static void initAdjListsMetadataForAnIndexTask(uint64_t numNodeOffsets,
        uint32_t numEdgesPerPage, listSizes_t *listSizes, AdjListsMetadata *adjListsIndexMetadata);

    static void populateAdjListsTask(RelLabelMetadata *relLabelMetadata, uint64_t blockId,
        const char tokenSeparator, dirLabelListSizes_t *dirLabelListSizes,
        dirLabelAdjListsMetadata_t *dirLabelAdjListsMetadata,
        vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>> *adjListsIndexes,
        vector<shared_ptr<NodeIDMap>> *nodeIDMaps, const Catalog *catalog, bool hasProperties,
        shared_ptr<spdlog::logger> logger);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool &threadPool;
    const Graph &graph;
    const Catalog &catalog;
    const nlohmann::json &metadata;
    vector<shared_ptr<NodeIDMap>> &nodeIDMaps;
    const string outputDirectory;
};

} // namespace common
} // namespace graphflow
