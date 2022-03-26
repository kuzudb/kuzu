#pragma once

#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"
#include "src/loader/include/in_mem_structure/column_utils.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/loader/include/label_description.h"
#include "src/loader/include/loader_task.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// This class helps RelsLoader to build AdjColumns and RelPropertyColumns for a particular rel
// label if FWD/BWD relMultiplicity is 1. InMemAdjAndPropertyColumnsBuilder exposes functions to
// construct columns step-by-step and populate in-memory pages (for AdjColumns and
// RelPropertyColumns) and finally save the in-mem data structures to the disk.
class InMemAdjAndPropertyColumnsBuilder : public InMemStructuresBuilderForRels, public ColumnUtils {

    typedef vector<vector<unique_ptr<InMemOverflowPages>>> labelPropertyIdxOverflowPages_t;
    typedef vector<vector<unique_ptr<InMemPropertyPages>>> labelPropertyIdxPropertyColumn_t;
    typedef vector<vector<unique_ptr<InMemAdjPages>>> directionLabelAdjColumn_t;

public:
    // Initialize the builder and construct relevant propertyColumns and adjColumns.
    InMemAdjAndPropertyColumnsBuilder(RelLabelDescription& description,
        TaskScheduler& taskScheduler, const Graph& graph, const string& outputDirectory);

    // Sets a neighbour nodeID of the given nodeID in a corresponding adjColumn. If direction=FWD,
    // adjCol[nodeIDs[FWD]] = nodeIDs[BWD], and vice-versa.
    void setRel(Direction direction, const vector<nodeID_t>& nodeIDs);

    // Sets a property of a rel in RelPropertyColumn at a given nodeID.
    void setProperty(const nodeID_t& nodeID, const uint32_t& propertyIdx, const uint8_t* val,
        const DataTypeID& type);

    // Sets a string property of a rel in RelPropertyColumn at a given nodeID.
    void setStringProperty(const nodeID_t& nodeID, const uint32_t& propertyIdx, const char* val,
        PageByteCursor& cursor);
    void setListProperty(const nodeID_t& nodeID, const uint32_t& propertyIdx,
        const Literal& listVal, PageByteCursor& cursor);

    void sortOverflowStrings(LoaderProgressBar* progressBar) override;

    void saveToFile(LoaderProgressBar* progressBar) override;

private:
    void buildInMemPropertyColumns(Direction direction);
    void buildInMemAdjColumns();

    // concurrent task

    static void sortOverflowStringsOfPropertyColumnTask(node_offset_t offsetStart,
        node_offset_t offsetEnd, InMemPropertyPages* propertyColumn,
        InMemOverflowPages* unorderedStringOverflow, InMemOverflowPages* orderedStringOverflow,
        LoaderProgressBar* progressBar);

private:
    directionLabelAdjColumn_t dirLabelAdjColumns{2};

    labelPropertyIdxOverflowPages_t labelPropertyIdxOverflowPages;
    labelPropertyIdxPropertyColumn_t labelPropertyIdxPropertyColumn;
};

} // namespace loader
} // namespace graphflow
