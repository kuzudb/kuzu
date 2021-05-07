#pragma once

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/adj_and_prop_structures_builder.h"
#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/thread_pool.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// This class helps RelsLoader to build AdjColumns and RelPropertyColumns for a particular rel
// label if FWD/BWD cardinality is 1. AdjAndPropertyColumnsBuilder exposes functions to construct
// columns step-by-step and populate in-memory pages (for AdjColumns and RelPropertyColumns) and
// finally save the in-mem data structures to the disk.
class AdjAndPropertyColumnsBuilder : public AdjAndPropertyStructuresBuilder {

    typedef vector<vector<unique_ptr<InMemStringOverflowPages>>> labelPropertyIdxStringOverflow_t;
    typedef vector<vector<unique_ptr<InMemPropertyPages>>> labelPropertyIdxPropertyColumn_t;
    typedef vector<vector<unique_ptr<InMemAdjPages>>> dirLabelAdjColumn_t;

public:
    // Initialize the builder and construct relevant propertyColumns and adjColumns.
    AdjAndPropertyColumnsBuilder(RelLabelDescription& description, ThreadPool& threadPool,
        const Graph& graph, const string outputDirectory);

    // Sets a neighbour nodeID of the given nodeID in a corresponding adjColumn. If dir=FWD,
    // adjCol[nodeIDs[FWD]] = nodeIDs[BWD], and vice-versa.
    void setRel(const Direction& dir, const vector<nodeID_t>& nodeIDs);

    // Sets a property of a rel in RelPropertyColumn at a given nodeID.
    void setProperty(const nodeID_t& nodeID, const uint32_t& propertyIdx, const uint8_t* val,
        const DataType& type);

    // Sets a string property of a rel in RelPropertyColumn at a given nodeID.
    void setStringProperty(
        const nodeID_t& nodeID, const uint32_t& propertyIdx, const char* val, PageCursor& cursor);

    void sortOverflowStrings() override;

    void saveToFile() override;

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

    unique_ptr<labelPropertyIdxStringOverflow_t> labelPropertyIdxStringOverflowPages;
    labelPropertyIdxPropertyColumn_t labelPropertyIdxPropertyColumn;
};

} // namespace loader
} // namespace graphflow
