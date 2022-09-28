#pragma once

#include "in_mem_structures_csv_copier.h"

#include "src/storage/index/include/in_mem_hash_index.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

namespace graphflow {
namespace storage {

class InMemNodeCSVCopier : public InMemStructuresCSVCopier {

public:
    InMemNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~InMemNodeCSVCopier() override = default;

    void copy();
    void saveToFile() override;

private:
    void initializeColumnsAndList();
    vector<string> countLinesPerBlockAndParseUnstrPropertyNames(uint64_t numStructuredProperties);
    void populateColumnsAndCountUnstrPropertyListSizes();
    void calcUnstrListsHeadersAndMetadata();
    void populateUnstrPropertyLists();

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists);
    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        PageByteCursor& stringOvfPagesCursor,
        unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        InMemUnstructuredLists* unstrPropertyLists);

    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        const vector<Property>& properties, vector<PageByteCursor>& overflowCursors,
        CSVReader& reader, uint64_t nodeOffset);
    template<DataTypeID DT>
    static void addIDsToIndex(InMemColumn* column, InMemHashIndex* hashIndex,
        node_offset_t startOffset, uint64_t numValues);
    static void populateIDIndex(InMemColumn* column, InMemHashIndex* IDIndex,
        node_offset_t startOffset, uint64_t numValues);
    static void skipFirstRowIfNecessary(
        uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader);

    // Concurrent tasks.
    static void populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
        uint64_t blockId, uint64_t offsetStart, InMemHashIndex* IDIndex,
        InMemNodeCSVCopier* copier);
    static void populateUnstrPropertyListsTask(
        uint64_t blockId, node_offset_t nodeOffsetStart, InMemNodeCSVCopier* copier);

private:
    NodeTableSchema* nodeTableSchema;
    uint64_t numNodes;
    vector<unique_ptr<InMemColumn>> structuredColumns;
    unique_ptr<InMemUnstructuredLists> unstrPropertyLists;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace graphflow
