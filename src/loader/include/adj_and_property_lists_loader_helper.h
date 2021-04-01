#pragma once

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/lists_loader_helper.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class AdjAndPropertyListsLoaderHelper {

    typedef vector<vector<unique_ptr<listSizes_t>>> dirLabelListSizes_t;

    typedef vector<vector<ListHeaders>> dirLabelListHeaders_t;

    typedef vector<vector<ListsMetadata>> dirLabelAdjListsMetadata_t;
    typedef vector<vector<unique_ptr<InMemAdjPages>>> dirLabelAdjLists_t;

    typedef vector<vector<vector<ListsMetadata>>> dirLabelPropertyIdxPropertyListsMetadata_t;
    typedef vector<vector<vector<unique_ptr<InMemPropertyPages>>>>
        dirLabelPropertyIdxPropertyLists_t;

    typedef vector<vector<vector<unique_ptr<InMemStringOverflowPages>>>>
        dirLabelPropertyIdxStringOverflowPages_t;

public:
    AdjAndPropertyListsLoaderHelper(RelLabelDescription& description, ThreadPool& threadPool,
        const Graph& graph, const Catalog& catalog, const string outputDirectory);

    inline void incrementListSize(const Direction& dir, const nodeID_t& nodeID) {
        ListsLoaderHelper::incrementListSize(
            *dirLabelListSizes[dir][nodeID.label], nodeID.offset, 1);
    }

    inline uint64_t decrementListSize(const Direction& dir, const nodeID_t& nodeID) {
        return ListsLoaderHelper::decrementListSize(
            *dirLabelListSizes[dir][nodeID.label], nodeID.offset, 1);
    }

    void buildAdjListsHeadersAndListsMetadata();

    void buildInMemStructures();

    void setRel(const uint64_t& pos, const Direction& dir, const vector<nodeID_t>& nodeIDs);

    void setProperty(const vector<uint64_t>& pos, const vector<nodeID_t>& nodeIDs,
        const uint32_t& propertyIdx, const uint8_t* val, const DataType& type);

    void setStringProperty(const vector<uint64_t>& pos, const vector<nodeID_t>& nodeIDs,
        const uint32_t& propertyIdx, const char* strVal, PageCursor& stringOverflowCursor);

    void sortOverflowStrings();

    void saveToFile();

private:
    void initAdjListHeaders();
    void initAdjListsAndPropertyListsMetadata();

    void buildInMemPropertyLists();
    void buildInMemAdjLists();

    // concurrent tasks

    static void sortOverflowStringsOfPropertyListsTask(node_offset_t offsetStart,
        node_offset_t offsetEnd, InMemPropertyPages* propertyLists, ListHeaders* adjListsHeaders,
        ListsMetadata* listsMetadata, InMemStringOverflowPages* unorederedStringOverflowPages,
        InMemStringOverflowPages* orderedStringOverflowPages, shared_ptr<spdlog::logger> logger);

private:
    dirLabelListSizes_t dirLabelListSizes{2};

    dirLabelListHeaders_t dirLabelAdjListHeaders{2};
    dirLabelAdjListsMetadata_t dirLabelAdjListsMetadata{2};
    dirLabelAdjLists_t dirLabelAdjLists{2};

    dirLabelPropertyIdxPropertyListsMetadata_t dirLabelPropertyIdxPropertyListsMetadata{2};
    dirLabelPropertyIdxPropertyLists_t dirLabelPropertyIdxPropertyLists{2};
    unique_ptr<vector<unique_ptr<InMemStringOverflowPages>>> propertyIdxUnordStringOverflowPages;
    unique_ptr<dirLabelPropertyIdxStringOverflowPages_t> dirLabelPropertyIdxStringOverflowPages;

    shared_ptr<spdlog::logger> logger;
    RelLabelDescription& description;

    ThreadPool& threadPool;
    const Graph& graph;
    const Catalog& catalog;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
