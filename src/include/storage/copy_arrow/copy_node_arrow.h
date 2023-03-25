#pragma once

#include "copy_structures_arrow.h"
#include "storage/index/hash_index_builder.h"
#include "storage/storage_structure/bm_backed_in_mem_column.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace storage {

class CopyNodeArrow : public CopyStructuresArrow {

public:
    CopyNodeArrow(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager* bufferManager)
        : CopyStructuresArrow{copyDescription, std::move(outputDirectory), taskScheduler, catalog,
              tableID},
          nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, bufferManager{bufferManager} {
    }

private:
    inline void updateTableStatistics() override {
        nodesStatisticsAndDeletedIDs->setNumTuplesForTable(tableSchema->tableID, numRows);
    }

    void initializeColumnsAndLists() override;

    void populateColumnsAndLists() override;

    void saveToFile() override;

    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(
        std::vector<std::unique_ptr<BMBackedInMemColumn>>& columns,
        std::vector<PageByteCursor>& overflowCursors,
        const std::vector<std::shared_ptr<T>>& arrow_columns, common::offset_t nodeOffset,
        uint64_t blockOffset, common::CopyDescription& copyDescription);

    template<typename T>
    static void populatePKIndex(BMBackedInMemColumn* pkColumn, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset, uint64_t numValues);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockIdx,
        uint64_t startOffset, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
        const std::vector<std::shared_ptr<T2>>& batchColumns, std::string filePath);

    template<typename T>
    arrow::Status assignCopyCSVTasks(arrow::csv::StreamingReader* csvStreamingReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status assignCopyParquetTasks(parquet::arrow::FileReader* parquetReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void appendPKIndex(
        BMBackedInMemColumn* pkColumn, common::offset_t offset, HashIndexBuilder<T>* pkIndex) {
        assert(false);
    }

private:
    std::vector<std::unique_ptr<BMBackedInMemColumn>> columns;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    BufferManager* bufferManager;
};

} // namespace storage
} // namespace kuzu
