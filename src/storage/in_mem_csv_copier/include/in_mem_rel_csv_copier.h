#pragma once

#include "in_mem_structures_csv_copier.h"

#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace storage {

using label_adj_in_mem_columns_map_t = unordered_map<label_t, unique_ptr<InMemAdjColumn>>;
using label_property_in_mem_lists_map_t = unordered_map<label_t, vector<unique_ptr<InMemLists>>>;
using label_adj_in_mem_lists_map_t = unordered_map<label_t, unique_ptr<InMemAdjLists>>;
using label_property_in_mem_columns_map_t = unordered_map<label_t, vector<unique_ptr<InMemColumn>>>;

class InMemRelCSVCopier : public InMemStructuresCSVCopier {

public:
    InMemRelCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, vector<uint64_t> maxNodeOffsetsPerNodeLabel,
        BufferManager* bufferManager, label_t labelID);

    ~InMemRelCSVCopier() override = default;

    void copy();

    void saveToFile() override;

private:
    void countLinesPerBlock();
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
        Transaction* transaction, const Catalog& catalog, vector<bool>& requireToReadLabels);
    static void putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
        vector<label_property_in_mem_columns_map_t>& directionLabelPropertyColumns,
        const vector<Property>& properties, vector<unique_ptr<InMemOverflowFile>>& overflowPages,
        vector<PageByteCursor>& overflowCursors, CSVReader& reader,
        const vector<nodeID_t>& nodeIDs);
    static void putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
        vector<label_property_in_mem_lists_map_t>& directionLabelPropertyLists,
        vector<label_adj_in_mem_lists_map_t>& directionLabelAdjLists,
        const vector<Property>& properties, vector<unique_ptr<InMemOverflowFile>>& overflowPages,
        vector<PageByteCursor>& overflowCursors, CSVReader& reader, const vector<nodeID_t>& nodeIDs,
        const vector<uint64_t>& reversePos);
    static void copyStringOverflowFromUnorderedToOrderedPages(gf_string_t* gfStr,
        PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
        InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile);
    static void copyListOverflowFromUnorderedToOrderedPages(gf_list_t* gfList,
        const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
        PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
        InMemOverflowFile* orderedOverflowFile);

    // Concurrent tasks.
    static void populateAdjColumnsAndCountRelsInAdjListsTask(
        uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier);
    static void populateAdjAndPropertyListsTask(
        uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier);
    static void sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
        InMemOverflowFile* unorderedOverflowPages, InMemOverflowFile* orderedOverflowPages);
    static void sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
        InMemLists* propertyLists, InMemOverflowFile* unorderedStringOverflowPages,
        InMemOverflowFile* orderedStringOverflowPages);

private:
    const vector<uint64_t> maxNodeOffsetsPerNodeLabel;
    uint64_t startRelID;
    RelLabel* relLabel;

    unique_ptr<Transaction> tmpReadTransaction;
    vector<unique_ptr<HashIndex>> IDIndexes;
    vector<vector<unique_ptr<atomic_uint64_vec_t>>> directionLabelListSizes{2};
    vector<unique_ptr<atomic_uint64_vec_t>> directionNumRelsPerLabel{2};
    vector<NodeIDCompressionScheme> directionNodeIDCompressionScheme{2};
    vector<label_adj_in_mem_columns_map_t> directionLabelAdjColumns{2};
    vector<label_property_in_mem_columns_map_t> directionLabelPropertyColumns{2};
    vector<label_adj_in_mem_lists_map_t> directionLabelAdjLists{2};
    vector<label_property_in_mem_lists_map_t> directionLabelPropertyLists{2};
    vector<unique_ptr<InMemOverflowFile>> propertyColumnsOverflowFiles;
    vector<unique_ptr<InMemOverflowFile>> propertyListsOverflowFiles;
};

} // namespace storage
} // namespace graphflow
