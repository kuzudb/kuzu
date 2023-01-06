#pragma once

#include "copy_structures_arrow.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class CopyNodeArrow : public CopyStructuresArrow {

public:
    CopyNodeArrow(CopyDescription& copyDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~CopyNodeArrow() override = default;

    uint64_t copy();

    void saveToFile() override;

private:
    void initializeColumnsAndList();

    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromArrow(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        vector<PageByteCursor>& overflowCursors, const std::vector<shared_ptr<T>>& arrow_columns,
        uint64_t nodeOffset, uint64_t bufferOffset, CopyDescription& copyDescription);

    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        node_offset_t startOffset, uint64_t numValues);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockId,
        uint64_t offsetStart, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
        const vector<shared_ptr<T2>>& batchColumns, CopyDescription& copyDescription);

private:
    NodeTableSchema* nodeTableSchema;
    vector<unique_ptr<InMemColumn>> columns;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace kuzu
