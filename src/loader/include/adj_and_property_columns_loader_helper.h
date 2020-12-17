#pragma once

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

typedef vector<vector<unique_ptr<InMemStringOverflowPages>>> labelPropertyIdxStringOverflow_t;
typedef vector<vector<unique_ptr<InMemPropertyPages>>> labelPropertyIdxPropertyColumn_t;
typedef vector<vector<unique_ptr<InMemAdjPages>>> dirLabelAdjColumn_t;

class AdjAndPropertyColumnsLoaderHelper {

public:
    AdjAndPropertyColumnsLoaderHelper(RelLabelDescription& description, ThreadPool& threadPool,
        const Graph& graph, const Catalog& catalog, const string outputDirectory,
        shared_ptr<spdlog::logger> logger);

    void setRel(const Direction& dir, const vector<nodeID_t>& nodeIDs);

    void setProperty(const nodeID_t& nodeID, const uint32_t& propertyIdx, const uint8_t* val,
        const DataType& type);

    void setStringProperty(
        const nodeID_t& nodeID, const uint32_t& propertyIdx, const char* val, PageCursor& cursor);

    void sortOverflowStrings();

    void saveToFile();

private:
    void buildInMemPropertyColumns(Direction dir);

    void buildInMemAdjColumns();

    static void calculatePageCursor(
        const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor);

    // concurrent task

    static void sortOverflowStringsofPropertyColumnTask(node_offset_t offsetStart,
        node_offset_t offsetEnd, InMemPropertyPages* propertyColumn,
        InMemStringOverflowPages* unorderedStringOverflow,
        InMemStringOverflowPages* orderedStringOverflow, shared_ptr<spdlog::logger> logger);

private:
    dirLabelAdjColumn_t dirLabelAdjColumns{2};

    unique_ptr<labelPropertyIdxStringOverflow_t> labelPropertyIdxStringOverflow;
    labelPropertyIdxPropertyColumn_t labelPropertyIdxPropertyColumn;

    shared_ptr<spdlog::logger> logger;
    RelLabelDescription& description;
    ThreadPool& threadPool;
    const Graph& graph;
    const Catalog& catalog;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
