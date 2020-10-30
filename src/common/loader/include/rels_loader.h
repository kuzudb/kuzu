#pragma once

#include "nlohmann/json.hpp"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/loader/include/thread_pool.h"
#include "src/common/loader/include/utils.h"
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
    vector<vector<shared_ptr<NodeIDMap>>> nodeIDMapsPerDir{2};
    vector<pair<uint32_t, uint32_t>> numBytesSchemePerDir{2};
};

// Holds auxiliary info an adjLists index.
struct AdjListsIndexMetadata {

    // synchronously counts the number of rels in a node offset.
    unique_ptr<vector<atomic<uint64_t>>> listSizes;

    // A header of a node is a uint32_t value the describes the following information about the
    // adjList of that node: 1) type: small or large {lAdjList} and, 2) location of adjList.

    // If adjList is small, the layout of header is:
    //      1. MSB (31st bit) is reset
    //      2. 30-22 bits denote the index in the chunkPagesMaps[chunkId] where disk page ID is
    //      located, where chunkId = nodeOffset / 512.
    //      3. 21-11 bits denote the offset in the disk page.
    //      4. 10-0  bits denote the number of elements in the adjlist.

    // If adjList is a large one (during initial data ingest, the criterion for being large is 
    // whether or not the adjList fits in a single page), the layout of header is:
    //      1. MSB (31st bit) is set
    //      2. 30-0  bits denote the idx in the lAdjListsPagesMaps where disk page IDs are located.
    vector<uint32_t> headers;

    // Holds the list of alloted disk page IDs for each chunk (that is a collection of regular
    // adjlists for 512 node offsets).
    vector<vector<uint64_t>> chunksPagesMap;

    // Holds the list of alloted disk page IDs for the
    // corresponding large adjList.
    vector<vector<uint64_t>> lAdjListsPagesMap;

    // total number of pages required for this adjListsIndex.
    uint64_t numPages;
};

typedef vector<vector<AdjListsIndexMetadata>> dirLabelAdjListsIndexMetadata_t;

namespace graphflow {
namespace common {

class RelsLoader {
    friend class GraphLoader;

private:
    RelsLoader(ThreadPool &threadPool, const Graph &graph, const Catalog &catalog,
        const nlohmann::json &metadata, const string outputDirectory)
        : logger{spdlog::stdout_logger_mt("relsLoader")}, threadPool{threadPool}, graph{graph},
          catalog{catalog}, metadata{metadata}, outputDirectory{outputDirectory} {};

    void load(vector<string> &fnames, unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps,
        vector<uint64_t> &numBlocksPerLabel);

    void loadRelsForLabel(RelLabelMetadata &relLabelMetadata);

    // adjEdges Indexes

    // constructs adjEdges indexes if the rel label is single cardinality in a direction or
    // otherwise, counts the rels in the adjLists of each node offset.
    void constructAdjEdgesAndCountRelsInAdjLists(RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // builds in-memory adjEdges indexes for all src node labels (if the rel label is single
    // cardinality in the forward direction) and the dst node labels (if the rel label is single
    // cardinality in the backward direction)
    shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> buildInMemAdjEdges(
        RelLabelMetadata &relLabelMetadata);

    // adjLists Indexes

    // constructs adjLists indexes if the rel label is multi cardinality in a direction.
    void contructAdjLists(RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // Allocate space in adjLists pages to all the adjLists indexes of a particular label.
    void allocateAdjListsPages(RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // Allocate space in adjLists pages to a single adjLists index. This function computes and
    // populates the headers of the index, sets the layout of pages and creates the
    // chunkPagesMap and lAdjListsPagesMap.
    uint64_t allocateAdjListsPagesForAnIndex(uint64_t numNodeOffsets, uint32_t numEdgesPerPage,
        AdjListsIndexMetadata &adjListsIndexMetadata);

    // Populates rels in adjLists indexes using the space allocation scheme of
    // allocateAdjListsPages().
    void populateAdjLists(RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // builds in-memory adjLists indexes for all src node labels (if the rel label is multi
    // cardinality in the forward direction) and the dst node labels (if the rel label is multi
    // cardinality in the backward direction)
    shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>> buildInMemAdjLists(
        RelLabelMetadata &relLabelMetadata,
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // writes auxiliary data of adjLists indexes {headers, chunkPagesMap and lAdjListsPagesMap}
    // to disk.
    void writeAdjListsHeadersAndPagesMaps(
        dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata);

    // Concurrent Tasks & Helpers

    static void populateAdjEdgesAndCountRelsInAdjListsTask(RelLabelMetadata *relLabelMetadata,
        uint64_t blockId, const char tokenSeparator,
        dirLabelAdjListsIndexMetadata_t *dirLabelAdjListsIndexMetadata,
        shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> adjEdgesIndexes,
        bool hasProperties, shared_ptr<spdlog::logger> logger);

    static void populateAdjListsTask(RelLabelMetadata *relLabelMetadata, uint64_t blockId,
        const char tokenSeparator, dirLabelAdjListsIndexMetadata_t *dirLabelAdjListsIndexMetadata,
        shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>> adjListsIndexes,
        bool hasProperties, shared_ptr<spdlog::logger> logger);

    // Task Helpers

    static void resolveNodeIDMapIdxAndOffset(string &nodeID, vector<gfLabel_t> &nodeLabels,
        vector<shared_ptr<NodeIDMap>> &nodeIDMaps, gfLabel_t &label, gfNodeOffset_t &offset);

private:
    shared_ptr<spdlog::logger> logger;
    ThreadPool &threadPool;
    const Graph &graph;
    const Catalog &catalog;
    const nlohmann::json &metadata;
    const string outputDirectory;
};

} // namespace common
} // namespace graphflow
