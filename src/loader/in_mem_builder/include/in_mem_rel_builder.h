#pragma once

#include "src/loader/in_mem_builder/include/in_mem_structures_builder.h"
#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace loader {

class InMemRelBuilder : public InMemStructuresBuilder {

public:
    InMemRelBuilder(label_t label, const RelFileDescription& fileDescription,
        string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerNodeLabel, BufferManager& bufferManager,
        LoaderProgressBar* progressBar);

    ~InMemRelBuilder() override = default;

    void load();

    void saveToFile() override;

private:
    void initializeColumnsAndLists();
    void initializeColumns(RelDirection relDirection);
    void initializeLists(RelDirection relDirection);
    void initAdjListsHeaders();
    void initAdjAndPropertyListsMetadata();

    void populateAdjColumnsAndCountRelsInAdjLists();
    void populateAdjAndPropertyLists();
    void populateNumRels();
    void sortOverflowValues();

    uint64_t getNumTasksOfInitializingAdjAndPropertyListsMetadata();
    static void inferLabelsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<DataType>& nodeIDTypes, const vector<unique_ptr<HashIndex>>& IDIndexes,
        const Catalog& catalog, vector<bool>& requireToReadLabels);
    static void putPropsOfLineIntoColumns(
        vector<label_property_columns_map_t>& directionLabelPropertyColumns,
        const vector<Property>& properties, vector<unique_ptr<InMemOverflowFile>>& overflowPages,
        vector<PageCursor>& overflowCursors, CSVReader& reader, const vector<nodeID_t>& nodeIDs);
    static void putPropsOfLineIntoLists(
        vector<label_property_lists_map_t>& directionLabelPropertyLists,
        vector<label_adj_lists_map_t>& directionLabelAdjLists, const vector<Property>& properties,
        vector<unique_ptr<InMemOverflowFile>>& overflowPages, vector<PageCursor>& overflowCursors,
        CSVReader& reader, const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos);
    static void copyStringOverflowFromUnorderedToOrderedPages(gf_string_t* gfStr,
        PageCursor& unorderedOverflowCursor, PageCursor& orderedOverflowCursor,
        InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile);
    static void copyListOverflowFromUnorderedToOrderedPages(gf_list_t* gfList,
        const DataType& dataType, PageCursor& unorderedOverflowCursor,
        PageCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
        InMemOverflowFile* orderedOverflowFile);

    // Concurrent tasks.
    static void populateAdjColumnsAndCountRelsInAdjListsTask(
        uint64_t blockId, InMemRelBuilder* builder);
    static void populateAdjAndPropertyListsTask(uint64_t blockId, InMemRelBuilder* builder);
    static void sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
        InMemOverflowFile* unorderedOverflowPages, InMemOverflowFile* orderedOverflowPages,
        LoaderProgressBar* progressBar);
    static void sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
        InMemLists* propertyLists, InMemOverflowFile* unorderedStringOverflowPages,
        InMemOverflowFile* orderedStringOverflowPages, LoaderProgressBar* progressBar);

private:
    RelMultiplicity relMultiplicity;
    vector<string> srcNodeLabelNames;
    vector<string> dstNodeLabelNames;
    const vector<uint64_t> maxNodeOffsetsPerNodeLabel;

    BufferManager& bm;
    vector<unique_ptr<HashIndex>> IDIndexes;
    vector<vector<unique_ptr<atomic_uint64_vec_t>>> directionLabelListSizes{2};
    vector<unique_ptr<atomic_uint64_vec_t>> directionNumRelsPerLabel{2};
    vector<NodeIDCompressionScheme> directionNodeIDCompressionScheme{2};
    vector<label_adj_columns_map_t> directionLabelAdjColumns{2};
    vector<label_property_columns_map_t> directionLabelPropertyColumns{2};
    vector<label_adj_lists_map_t> directionLabelAdjLists{2};
    vector<label_property_lists_map_t> directionLabelPropertyLists{2};
    vector<unique_ptr<InMemOverflowFile>> propertyColumnsOverflowFiles;
    vector<unique_ptr<InMemOverflowFile>> propertyListsOverflowFiles;
};

} // namespace loader
} // namespace graphflow
