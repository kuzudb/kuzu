#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "storage/stats/node_table_statistics.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node table).
// Note: This class is *not* thread-safe.
class NodesStoreStatsAndDeletedIDs : public TablesStatistics {
public:
    // Should only be used by saveInitialNodesStatisticsAndDeletedIDsToFile to start a database
    // from an empty directory.
    explicit NodesStoreStatsAndDeletedIDs(common::VirtualFileSystem* vfs)
        : TablesStatistics{nullptr /* metadataFH */, nullptr /* bufferManager */, nullptr /* wal */,
              vfs} {};
    // Should be used when an already loaded database is started from a directory.
    NodesStoreStatsAndDeletedIDs(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        common::VirtualFileSystem* vfs,
        common::FileVersionType dbFileType = common::FileVersionType::ORIGINAL)
        : TablesStatistics{metadataFH, bufferManager, wal, vfs} {
        readFromFile(dbFileType);
    }

    inline NodeTableStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs(
        transaction::Transaction* transaction, common::table_id_t tableID) const {
        return getNodeTableStats(transaction->getType(), tableID);
    }

    static inline void saveInitialNodesStatisticsAndDeletedIDsToFile(common::VirtualFileSystem* vfs,
        const std::string& directory) {
        std::make_unique<NodesStoreStatsAndDeletedIDs>(vfs)->saveToFile(directory,
            common::FileVersionType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    void updateNumTuplesByValue(common::table_id_t tableID, int64_t value) override;

    common::offset_t getMaxNodeOffset(transaction::Transaction* transaction,
        common::table_id_t tableID);

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeStatisticsAndDeleteIDsPerTable() const {
        return readOnlyVersion->tableStatisticPerTable.size();
    }

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    inline common::offset_t addNode(common::table_id_t tableID) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
        setToUpdated();
        return getNodeTableStats(transaction::TransactionType::WRITE, tableID)->addNode();
    }

    // Refer to the comments for addNode.
    inline void deleteNode(common::table_id_t tableID, common::offset_t nodeOffset) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
        setToUpdated();
        getNodeTableStats(transaction::TransactionType::WRITE, tableID)->deleteNode(nodeOffset);
    }

    void setDeletedNodeOffsetsForMorsel(transaction::Transaction* tx,
        common::ValueVector* nodeIDVector, common::table_id_t tableID);

    void addNodeStatisticsAndDeletedIDs(catalog::NodeTableCatalogEntry* nodeTableEntry);

    void addMetadataDAHInfo(common::table_id_t tableID, const common::LogicalType& dataType);
    void removeMetadataDAHInfo(common::table_id_t tableID, common::column_id_t columnID);
    MetadataDAHInfo* getMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::column_id_t columnID);

protected:
    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableCatalogEntry* tableEntry) override {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(metadataFH, *tableEntry, bufferManager,
            wal);
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(
            *(NodeTableStatsAndDeletedIDs*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(const std::string& directory,
        common::FileVersionType dbFileType) override {
        return StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(vfs, directory, dbFileType);
    }

private:
    inline NodeTableStatsAndDeletedIDs* getNodeTableStats(
        transaction::TransactionType transactionType, common::table_id_t tableID) const {
        return transactionType == transaction::TransactionType::READ_ONLY ?
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       readOnlyVersion->tableStatisticPerTable.at(tableID).get()) :
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       readWriteVersion->tableStatisticPerTable.at(tableID).get());
    }
};
} // namespace storage
} // namespace kuzu
