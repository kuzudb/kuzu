#pragma once

#include "in_mem_structures_csv_copier.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class InMemNodeCSVCopier : public InMemStructuresCSVCopier {

public:
    InMemNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~InMemNodeCSVCopier() override = default;

    uint64_t copy();
    void saveToFile() override;

private:
    void initializeColumnsAndList();
    void countLinesPerBlock(uint64_t numStructuredProperties);
    template<typename T>
    void populateColumns();

    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        const vector<Property>& properties, vector<PageByteCursor>& overflowCursors,
        CSVReader& reader, uint64_t nodeOffset);
    template<typename T>
    static void addIDsToIndex(InMemColumn* column, HashIndexBuilder<T>* hashIndex,
        node_offset_t startOffset, uint64_t numValues);
    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        node_offset_t startOffset, uint64_t numValues);
    static void skipFirstRowIfNecessary(
        uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    // Instead, it is the index in the structured columns that we expect it to appear.
    template<typename T>
    static void populateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockId,
        uint64_t offsetStart, HashIndexBuilder<T>* pkIndex, InMemNodeCSVCopier* copier);

private:
    NodeTableSchema* nodeTableSchema;
    uint64_t numNodes;
    vector<unique_ptr<InMemColumn>> structuredColumns;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace kuzu
