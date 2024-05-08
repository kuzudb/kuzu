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
    // Should be used when an already loaded database is started from a directory.
    NodesStoreStatsAndDeletedIDs(const std::string& databasePath, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, common::VirtualFileSystem* fs);

    NodeTableStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs(
        transaction::Transaction* transaction, common::table_id_t tableID) const {
        return getNodeTableStats(transaction->getType(), tableID);
    }

    void updateNumTuplesByValue(common::table_id_t tableID, int64_t value) override;

    common::offset_t getMaxNodeOffset(transaction::Transaction* transaction,
        common::table_id_t tableID);

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    common::offset_t addNode(common::table_id_t tableID);

    // Refer to the comments for addNode.
    void deleteNode(common::table_id_t tableID, common::offset_t nodeOffset);

    void setDeletedNodeOffsetsForMorsel(transaction::Transaction* tx,
        common::ValueVector* nodeIDVector, common::table_id_t tableID);

    void addNodeStatisticsAndDeletedIDs(catalog::NodeTableCatalogEntry* nodeTableEntry);

    void addMetadataDAHInfo(common::table_id_t tableID, const common::LogicalType& dataType);
    void removeMetadataDAHInfo(common::table_id_t tableID, common::column_id_t columnID);
    MetadataDAHInfo* getMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::column_id_t columnID);

protected:
    std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableCatalogEntry* tableEntry) override {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(metadataFH, *tableEntry, bufferManager,
            wal);
    }

    std::string getTableStatisticsFilePath(const std::string& directory,
        common::FileVersionType dbFileType, common::VirtualFileSystem* fs) override {
        return StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(fs, directory, dbFileType);
    }

private:
    NodeTableStatsAndDeletedIDs* getNodeTableStats(transaction::TransactionType transactionType,
        common::table_id_t tableID) const {
        return transactionType == transaction::TransactionType::READ_ONLY ?
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       readOnlyVersion->tableStatisticPerTable.at(tableID).get()) :
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       readWriteVersion->tableStatisticPerTable.at(tableID).get());
    }
};
} // namespace storage
} // namespace kuzu
