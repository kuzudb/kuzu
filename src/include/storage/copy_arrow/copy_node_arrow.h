#pragma once

#include "copy_structures_arrow.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class CopyNodeArrow : public CopyStructuresArrow {

public:
    CopyNodeArrow(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~CopyNodeArrow() override = default;

    uint64_t copy();

    void saveToFile() override;

private:
    void initializeColumnsAndList();

    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromArrow(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(std::vector<std::unique_ptr<InMemColumn>>& columns,
        std::vector<PageByteCursor>& overflowCursors,
        const std::vector<std::shared_ptr<T>>& arrow_columns, uint64_t nodeOffset,
        uint64_t bufferOffset, common::CopyDescription& copyDescription);

    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset, uint64_t numValues);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockId,
        uint64_t offsetStart, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
        const std::vector<std::shared_ptr<T2>>& batchColumns,
        common::CopyDescription& copyDescription);

private:
    catalog::NodeTableSchema* nodeTableSchema;
    std::vector<std::unique_ptr<InMemColumn>> columns;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace kuzu
