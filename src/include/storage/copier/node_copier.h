#pragma once

#include "copy_structures_arrow.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class NodeCopier : public CopyStructuresArrow {

public:
    NodeCopier(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : CopyStructuresArrow{copyDescription, std::move(outputDirectory), taskScheduler, catalog,
              tableID},
          nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {}

protected:
    inline void updateTableStatistics() override {
        nodesStatisticsAndDeletedIDs->setNumTuplesForTable(tableSchema->tableID, numRows);
    }

    void initializeColumnsAndLists() override;

    void populateColumnsAndLists() override;

    void saveToFile() override;

    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset, uint64_t numValues);

    std::unordered_map<common::property_id_t, std::unique_ptr<InMemColumn>> columns;

private:
    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(
        std::unordered_map<common::property_id_t, std::unique_ptr<InMemColumn>>& columns,
        std::vector<PageByteCursor>& overflowCursors,
        const std::vector<std::shared_ptr<T>>& arrow_columns, uint64_t nodeOffset,
        uint64_t bufferOffset, common::CopyDescription& copyDescription);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockIdx,
        uint64_t startOffset, HashIndexBuilder<T1>* pkIndex, NodeCopier* copier,
        const std::vector<std::shared_ptr<T2>>& batchColumns, std::string filePath);

    template<typename T>
    arrow::Status assignCopyCSVTasks(arrow::csv::StreamingReader* csvStreamingReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status assignCopyParquetTasks(parquet::arrow::FileReader* parquetReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace kuzu
