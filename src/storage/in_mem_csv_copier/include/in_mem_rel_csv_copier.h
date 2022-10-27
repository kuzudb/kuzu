#pragma once

#include "in_mem_structures_csv_copier.h"

#include "src/storage/index/include/hash_index.h"
#include "src/storage/store/include/rels_statistics.h"

namespace graphflow {
namespace storage {

using table_adj_in_mem_columns_map_t = unordered_map<table_id_t, unique_ptr<InMemAdjColumn>>;
using table_property_in_mem_lists_map_t = unordered_map<table_id_t, vector<unique_ptr<InMemLists>>>;
using table_adj_in_mem_lists_map_t = unordered_map<table_id_t, unique_ptr<InMemAdjLists>>;
using table_property_in_mem_columns_map_t =
    unordered_map<table_id_t, vector<unique_ptr<InMemColumn>>>;

class InMemRelCSVCopier : public InMemStructuresCSVCopier {

public:
    InMemRelCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog,
        map<table_id_t, node_offset_t> maxNodeOffsetsPerNodeTable, BufferManager* bufferManager,
        table_id_t tableID, RelsStatistics* relsStatistics);

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
    void sortOverflowValues();

    static void inferTableIDsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<DataType>& nodeIDTypes, const map<table_id_t, unique_ptr<HashIndex>>& IDIndexes,
        Transaction* transaction, const Catalog& catalog, vector<bool> requireToReadTableLabels);
    static void putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
        vector<table_property_in_mem_columns_map_t>& directionTablePropertyColumns,
        const vector<Property>& properties,
        vector<unique_ptr<InMemOverflowFile>>& inMemOverflowFile,
        vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
        const vector<nodeID_t>& nodeIDs);
    static void putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
        vector<table_property_in_mem_lists_map_t>& directionTablePropertyLists,
        vector<table_adj_in_mem_lists_map_t>& directionTableAdjLists,
        const vector<Property>& properties,
        vector<unique_ptr<InMemOverflowFile>>& inMemOverflowFiles,
        vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
        const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos);
    static void copyStringOverflowFromUnorderedToOrderedPages(gf_string_t* gfStr,
        PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
        InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile);
    static void copyListOverflowFromUnorderedToOrderedPages(gf_list_t* gfList,
        const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
        PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
        InMemOverflowFile* orderedOverflowFile);
    static void skipFirstRowIfNecessary(
        uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader);
    static vector<bool> getTableLabelConfig(
        vector<bool> requireToReadTableLabels, InMemRelCSVCopier* copier);

    // Concurrent tasks.
    static void populateAdjColumnsAndCountRelsInAdjListsTask(
        uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier);
    static void populateAdjAndPropertyListsTask(
        uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier);
    static void sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemColumn* propertyColumn,
        InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile);
    static void sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
        node_offset_t offsetStart, node_offset_t offsetEnd, InMemAdjLists* adjLists,
        InMemLists* propertyLists, InMemOverflowFile* unorderedInMemOverflowFile,
        InMemOverflowFile* orderedInMemOverflowFile);

private:
    const map<table_id_t, node_offset_t> maxNodeOffsetsPerTable;
    uint64_t startRelID;
    RelTableSchema* relTableSchema;
    RelsStatistics* relsStatistics;
    unique_ptr<Transaction> dummyReadOnlyTrx;
    map<table_id_t, unique_ptr<HashIndex>> IDIndexes;
    vector<map<table_id_t, unique_ptr<atomic_uint64_vec_t>>> directionTableListSizes{2};
    vector<map<table_id_t, atomic<uint64_t>>> directionNumRelsPerTable{2};
    vector<NodeIDCompressionScheme> directionNodeIDCompressionScheme{2};
    vector<table_adj_in_mem_columns_map_t> directionTableAdjColumns{2};
    vector<table_property_in_mem_columns_map_t> directionTablePropertyColumns{2};
    vector<table_adj_in_mem_lists_map_t> directionTableAdjLists{2};
    vector<table_property_in_mem_lists_map_t> directionTablePropertyLists{2};
    vector<unique_ptr<InMemOverflowFile>> propertyColumnsOverflowFiles;
    vector<unique_ptr<InMemOverflowFile>> propertyListsOverflowFiles;
};

} // namespace storage
} // namespace graphflow
