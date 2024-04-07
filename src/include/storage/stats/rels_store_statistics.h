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
    explicit RelsStoreStats(common::VirtualFileSystem* vfs)
        : TablesStatistics{nullptr /* metadataFH */, nullptr /* bufferManager */, nullptr /* wal */,
              vfs} {};
    // Should be used when an already loaded database is started from a directory.
    RelsStoreStats(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        common::VirtualFileSystem* vfs);

    static inline void saveInitialRelsStatisticsToFile(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        std::make_unique<RelsStoreStats>(vfs)->saveToFile(directory,
            common::FileVersionType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    inline RelTableStats* getRelStatistics(common::table_id_t tableID,
        transaction::Transaction* transaction) const {
        auto& tableStatisticPerTable =
            transaction->getType() == transaction::TransactionType::READ_ONLY ?
                readOnlyVersion->tableStatisticPerTable :
                readWriteVersion->tableStatisticPerTable;
        KU_ASSERT(tableStatisticPerTable.contains(tableID));
        return (RelTableStats*)tableStatisticPerTable[tableID].get();
    }

    void updateNumTuplesByValue(common::table_id_t relTableID, int64_t value) override;

    common::offset_t getNextRelOffset(transaction::Transaction* transaction,
        common::table_id_t tableID);

    void addMetadataDAHInfo(common::table_id_t tableID, const common::LogicalType& dataType);
    void removeMetadataDAHInfo(common::table_id_t tableID, common::column_id_t columnID);
    MetadataDAHInfo* getCSROffsetMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::RelDataDirection direction);
    MetadataDAHInfo* getCSRLengthMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::RelDataDirection direction);
    MetadataDAHInfo* getColumnMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::column_id_t columnID,
        common::RelDataDirection direction);

protected:
    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableCatalogEntry* tableEntry) override {
        return std::make_unique<RelTableStats>(metadataFH, *tableEntry, bufferManager, wal);
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<RelTableStats>(*(RelTableStats*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(const std::string& directory,
        common::FileVersionType dbFileType) override {
        return StorageUtils::getRelsStatisticsFilePath(vfs, directory, dbFileType);
    }

    inline void increaseNextRelOffset(common::table_id_t relTableID, uint64_t numTuples) {
        ((RelTableStats*)readWriteVersion->tableStatisticPerTable.at(relTableID).get())
            ->incrementNextRelOffset(numTuples);
    }
};

} // namespace storage
} // namespace kuzu
