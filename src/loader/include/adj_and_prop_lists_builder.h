#pragma once

#include "src/loader/include/adj_and_prop_structures_builder.h"
#include "src/loader/include/in_mem_pages.h"
#include "src/loader/include/loader_task.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// This builder class helps RelsLoader to build AdjLists and RelPropertyLists for a particular rel
// label if FWD/BWD relMultiplicity is not 1. Similar to
// AdjAndPropertyColsBuilderAndListSizeCounter, this also exposes functions to construct Lists
// step-by-step and populate in-memory pages and finally save the in-mem data structures to the
// disk.
class AdjAndPropertyListsBuilder : public AdjAndPropertyStructuresBuilder {

    typedef vector<vector<unique_ptr<listSizes_t>>> directionLabelListSizes_t;

    typedef vector<vector<ListHeaders>> directionLabelListHeaders_t;

    typedef vector<vector<ListsMetadata>> directionLabelAdjListsMetadata_t;
    typedef vector<vector<unique_ptr<InMemAdjPages>>> directionLabelAdjLists_t;

    typedef vector<vector<vector<ListsMetadata>>> directionLabelPropertyIdxPropertyListsMetadata_t;
    typedef vector<vector<vector<unique_ptr<InMemPropertyPages>>>>
        directionLabelPropertyIdxPropertyLists_t;

    typedef vector<vector<vector<unique_ptr<InMemStringOverflowPages>>>>
        directionLabelPropertyIdxStringOverflowPages_t;

public:
    AdjAndPropertyListsBuilder(RelLabelDescription& description, TaskScheduler& taskScheduler,
        const Graph& graph, const string& outputDirectory);

    inline void incrementListSize(const Direction& direction, const nodeID_t& nodeID) {
        ListsLoaderHelper::incrementListSize(
            *directionLabelListSizes[direction][nodeID.label], nodeID.offset, 1);
        (*directionLabelNumRels[direction])[nodeID.label]++;
    }

    inline uint64_t decrementListSize(const Direction& direction, const nodeID_t& nodeID) {
        return ListsLoaderHelper::decrementListSize(
            *directionLabelListSizes[direction][nodeID.label], nodeID.offset, 1);
    }

    // Should be called after the listSizes has been updated. Encodes the header info of each list
    // in all AdjLists structures.
    void buildAdjListsHeadersAndListsMetadata();

    // should be called after ListHeaders have been created. Calculates the metadata info of each
    // Lists structure (AdjLists / RelPropertyLists) besed on its relevant headers info.
    void buildInMemStructures();

    // Sets a neighbour nodeID in the adjList of the given nodeID in a particular adjLists
    // structure.
    void setRel(uint64_t pos, Direction direction, const vector<nodeID_t>& nodeIDs);

    // Sets a proeprty in the propertyList of the given nodeID in a particular RelPropertyLists
    // structure.
    void setProperty(const vector<uint64_t>& pos, const vector<nodeID_t>& nodeIDs,
        uint32_t propertyIdx, const uint8_t* val, DataType type);

    // Sets a string proeprty in the propertyList of the given nodeID in a particular
    // RelPropertyLists structure.
    void setStringProperty(const vector<uint64_t>& pos, const vector<nodeID_t>& nodeIDs,
        const uint32_t& propertyIdx, const char* strVal, PageCursor& stringOverflowCursor);

    void sortOverflowStrings() override;

    void saveToFile() override;

private:
    void initAdjListHeaders();
    void initAdjListsAndPropertyListsMetadata();

    void buildInMemPropertyLists();
    void buildInMemAdjLists();

    // concurrent tasks

    static void sortOverflowStringsOfPropertyListsTask(node_offset_t offsetStart,
        node_offset_t offsetEnd, InMemPropertyPages* propertyLists, ListHeaders* adjListsHeaders,
        ListsMetadata* listsMetadata, InMemStringOverflowPages* unorederedStringOverflowPages,
        InMemStringOverflowPages* orderedStringOverflowPages);

private:
    directionLabelListSizes_t directionLabelListSizes{2};

    directionLabelListHeaders_t directionLabelAdjListHeaders{2};
    directionLabelAdjListsMetadata_t directionLabelAdjListsMetadata{2};
    directionLabelAdjLists_t directionLabelAdjLists{2};

    directionLabelPropertyIdxPropertyListsMetadata_t directionLabelPropertyIdxPropertyListsMetadata{
        2};
    directionLabelPropertyIdxPropertyLists_t directionLabelPropertyIdxPropertyLists{2};
    unique_ptr<vector<unique_ptr<InMemStringOverflowPages>>> propertyIdxUnordStringOverflowPages;
    unique_ptr<directionLabelPropertyIdxStringOverflowPages_t>
        directionLabelPropertyIdxStringOverflowPages;
};

} // namespace loader
} // namespace graphflow
