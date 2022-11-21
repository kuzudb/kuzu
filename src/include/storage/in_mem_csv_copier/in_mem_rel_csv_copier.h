#pragma once

#include "in_mem_structures_csv_copier.h"
#include "storage/index/hash_index.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
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

    uint64_t copy();

    void saveToFile() override;

private:
    void countLinesPerBlock();
    void initializeColumnsAndLists();
    void initializeColumns(RelDirection relDirection);
    void initializeLists(RelDirection relDirection);
    void initAdjListsHeaders();
    void initListsMetadata();

    void populateAdjColumnsAndCountRelsInAdjLists();
    void populateLists();
    // We store rel properties with overflows, e.g., strings or lists, in
    // InMemColumn/ListsWithOverflowFile (e.g., InMemStringLists). When loading these properties
    // from csv, we first save the overflow pointers of the ku_list_t or ku_string_t in temporary
    // "unordered" InMemOverflowFiles in an unordered format, instead of the InMemOverflowFiles of
    // the actual InMemColumn/ListsWithOverflowFile. In these temporary unordered
    // InMemOverflowFiles, the string of nodeOffset100's overflow data may be stored before
    // nodeOffset10's overflow data). This is because the each thread gets a chunk of the csv file
    // storing rels but rel files are not necessarily sorted by source or destination node IDs.
    // Therefore, after populating an InMemColumn/ListWithOverflowFile we have to do two things: (1)
    // we need to copy over the data in these temporary unordered InMemOverflowFile to the
    // InMemOverflowFiles of the InMemColumn/ListsWithOverflowFile. (2) To increase the performance
    // of scanning these overflow files, we also sort the overflow pointers based on nodeOffsets, so
    // when scanning rels of consecutive nodes, the overflows of these rels appear consecutively on
    // disk.
    void sortAndCopyOverflowValues();

    static void inferTableIDsAndOffsets(CSVReader& reader, vector<nodeID_t>& nodeIDs,
        vector<DataType>& nodeIDTypes,
        const map<table_id_t, unique_ptr<PrimaryKeyIndex>>& pkIndexes, Transaction* transaction,
        const Catalog& catalog, vector<bool> requireToReadTableLabels);
    static void putPropsOfLineIntoColumns(uint32_t numPropertiesToRead,
        vector<table_property_in_mem_columns_map_t>& directionTablePropertyColumns,
        const vector<Property>& properties,
        unordered_map<uint32_t, unique_ptr<InMemOverflowFile>>& inMemOverflowFilePerPropertyID,
        vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
        const vector<nodeID_t>& nodeIDs);
    static void putPropsOfLineIntoLists(uint32_t numPropertiesToRead,
        vector<table_property_in_mem_lists_map_t>& directionTablePropertyLists,
        vector<table_adj_in_mem_lists_map_t>& directionTableAdjLists,
        const vector<Property>& properties,
        unordered_map<uint32_t, unique_ptr<InMemOverflowFile>>& inMemOverflowFilesPerProperty,
        vector<PageByteCursor>& inMemOverflowFileCursors, CSVReader& reader,
        const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos);
    static void copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
        PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
        InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile);
    static void copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
        const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
        PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
        InMemOverflowFile* orderedOverflowFile);
    static void skipFirstRowIfNecessary(
        uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader);

    // Concurrent tasks.
    static void populateAdjColumnsAndCountRelsInAdjListsTask(
        uint64_t blockId, uint64_t blockStartRelID, InMemRelCSVCopier* copier);
    static void populateListsTask(
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
    map<table_id_t, unique_ptr<PrimaryKeyIndex>> pkIndexes;
    vector<map<table_id_t, unique_ptr<atomic_uint64_vec_t>>> directionTableListSizes{2};
    vector<map<table_id_t, atomic<uint64_t>>> directionNumRelsPerTable{2};
    vector<map<table_id_t, NodeIDCompressionScheme>> directionNodeIDCompressionScheme{2};
    vector<table_adj_in_mem_columns_map_t> directionTableAdjColumns{2};
    vector<table_property_in_mem_columns_map_t> directionTablePropertyColumns{2};
    vector<table_adj_in_mem_lists_map_t> directionTableAdjLists{2};
    vector<table_property_in_mem_lists_map_t> directionTablePropertyLists{2};
    unordered_map<uint32_t, unique_ptr<InMemOverflowFile>> overflowFilePerPropertyID;
};

} // namespace storage
} // namespace kuzu
