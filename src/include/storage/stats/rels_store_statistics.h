#pragma once

#include "storage/stats/rel_table_statistics.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// Manages the disk image of the numRels and numRelsPerDirectionBoundTable.
class RelsStoreStats : public TablesStatistics {
public:
    // Should only be used by saveInitialRelsStatisticsToFile to start a database from an empty
    // directory.
    RelsStoreStats() : TablesStatistics{nullptr, nullptr, nullptr} {};
    // Should be used when an already loaded database is started from a directory.
    RelsStoreStats(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal);

    // Should only be used by tests.
    explicit RelsStoreStats(std::unordered_map<common::table_id_t, std::unique_ptr<RelTableStats>>
            relStatisticPerTable_);

    static inline void saveInitialRelsStatisticsToFile(const std::string& directory) {
        std::make_unique<RelsStoreStats>()->saveToFile(
            directory, common::FileVersionType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    inline RelTableStats* getRelStatistics(
        common::table_id_t tableID, transaction::Transaction* transaction) const {
        auto& tableStatisticPerTable =
            transaction->getType() == transaction::TransactionType::READ_ONLY ?
                tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable :
                tablesStatisticsContentForWriteTrx->tableStatisticPerTable;
        KU_ASSERT(tableStatisticPerTable.contains(tableID));
        return (RelTableStats*)tableStatisticPerTable[tableID].get();
    }

    void setNumTuplesForTable(common::table_id_t relTableID, uint64_t numRels) override;

    void updateNumRelsByValue(common::table_id_t relTableID, int64_t value);

    common::offset_t getNextRelOffset(
        transaction::Transaction* transaction, common::table_id_t tableID);

    void addMetadataDAHInfo(common::table_id_t tableID, const common::LogicalType& dataType);
    void removeMetadataDAHInfo(common::table_id_t tableID, common::column_id_t columnID);
    MetadataDAHInfo* getCSROffsetMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::RelDataDirection direction);
    MetadataDAHInfo* getAdjMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::RelDataDirection direction);
    MetadataDAHInfo* getPropertyMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::column_id_t columnID,
        common::RelDataDirection direction);

protected:
    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) override {
        return std::make_unique<RelTableStats>(metadataFH, *tableSchema, bufferManager, wal);
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<RelTableStats>(*(RelTableStats*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::FileVersionType dbFileType) override {
        return StorageUtils::getRelsStatisticsFilePath(directory, dbFileType);
    }

    inline void increaseNextRelOffset(common::table_id_t relTableID, uint64_t numTuples) {
        ((RelTableStats*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(relTableID)
                .get())
            ->incrementNextRelOffset(numTuples);
    }
};

} // namespace storage
} // namespace kuzu
