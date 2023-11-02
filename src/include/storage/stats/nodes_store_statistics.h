#pragma once

#include "catalog/node_table_schema.h"
#include "storage/stats/node_table_statistics.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node table).
// Note: This class is *not* thread-safe.
class NodesStoreStatsAndDeletedIDs : public TablesStatistics {
public:
    // Should only be used by saveInitialNodesStatisticsAndDeletedIDsToFile to start a database
    // from an empty directory.
    NodesStoreStatsAndDeletedIDs() : TablesStatistics{nullptr, nullptr, nullptr} {};
    // Should be used when an already loaded database is started from a directory.
    NodesStoreStatsAndDeletedIDs(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        common::FileVersionType dbFileType = common::FileVersionType::ORIGINAL)
        : TablesStatistics{metadataFH, bufferManager, wal} {
        readFromFile(wal->getDirectory(), dbFileType);
    }

    // Should be used only by tests;
    explicit NodesStoreStatsAndDeletedIDs(
        std::unordered_map<common::table_id_t, std::unique_ptr<NodeTableStatsAndDeletedIDs>>&
            nodesStatisticsAndDeletedIDs);

    inline NodeTableStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs(
        common::table_id_t tableID) const {
        return getNodeTableStats(transaction::TransactionType::READ_ONLY, tableID);
    }

    static inline void saveInitialNodesStatisticsAndDeletedIDsToFile(const std::string& directory) {
        std::make_unique<NodesStoreStatsAndDeletedIDs>()->saveToFile(
            directory, common::FileVersionType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    inline void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) override {
        initTableStatisticsForWriteTrx();
        getNodeTableStats(transaction::TransactionType::WRITE, tableID)->setNumTuples(numTuples);
    }

    common::offset_t getMaxNodeOffset(
        transaction::Transaction* transaction, common::table_id_t tableID);

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeStatisticsAndDeleteIDsPerTable() const {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.size();
    }

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    inline common::offset_t addNode(common::table_id_t tableID) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        return getNodeTableStats(transaction::TransactionType::WRITE, tableID)->addNode();
    }

    // Refer to the comments for addNode.
    inline void deleteNode(common::table_id_t tableID, common::offset_t nodeOffset) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        getNodeTableStats(transaction::TransactionType::WRITE, tableID)->deleteNode(nodeOffset);
    }

    // This function is only used by storageManager to construct relsStore during start-up, so
    // we can just safely return the maxNodeOffsetPerTable for readOnlyVersion.
    std::map<common::table_id_t, common::offset_t> getMaxNodeOffsetPerTable() const;

    void setDeletedNodeOffsetsForMorsel(transaction::Transaction* transaction,
        const std::shared_ptr<common::ValueVector>& nodeOffsetVector, common::table_id_t tableID);

    void addNodeStatisticsAndDeletedIDs(catalog::NodeTableSchema* tableSchema);

    void addMetadataDAHInfo(common::table_id_t tableID, const common::LogicalType& dataType);
    void removeMetadataDAHInfo(common::table_id_t tableID, common::column_id_t columnID);
    MetadataDAHInfo* getMetadataDAHInfo(transaction::Transaction* transaction,
        common::table_id_t tableID, common::column_id_t columnID);

protected:
    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) override {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(
            metadataFH, *tableSchema, bufferManager, wal);
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(
            *(NodeTableStatsAndDeletedIDs*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::FileVersionType dbFileType) override {
        return StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(directory, dbFileType);
    }

private:
    inline NodeTableStatsAndDeletedIDs* getNodeTableStats(
        transaction::TransactionType transactionType, common::table_id_t tableID) const {
        return transactionType == transaction::TransactionType::READ_ONLY ?
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.at(tableID)
                           .get()) :
                   dynamic_cast<NodeTableStatsAndDeletedIDs*>(
                       tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(tableID)
                           .get());
    }
};
} // namespace storage
} // namespace kuzu
