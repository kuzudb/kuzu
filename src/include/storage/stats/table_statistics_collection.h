#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/table_statistics.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

struct TablesStatisticsContent {
    std::unordered_map<common::table_id_t, std::unique_ptr<TableStatistics>> tableStatisticPerTable;

    const TableStatistics* getTableStat(common::table_id_t tableID) const {
        KU_ASSERT(tableStatisticPerTable.contains(tableID));
        return tableStatisticPerTable.at(tableID).get();
    }
};

class WAL;
class TablesStatistics {
public:
    TablesStatistics(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal);

    virtual ~TablesStatistics() = default;

    // Return the num of tuples before update.
    virtual void updateNumTuplesByValue(common::table_id_t tableID, int64_t value) = 0;

    void writeTablesStatisticsFileForWALRecord(const std::string& directory,
        common::VirtualFileSystem* fs) {
        saveToFile(directory, common::FileVersionType::WAL_VERSION,
            transaction::TransactionType::WRITE, fs);
    }

    bool hasUpdates() const { return isUpdated; }

    void checkpointInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        KU_ASSERT(readWriteVersion);
        readOnlyVersion = std::move(readWriteVersion);
        resetToNotUpdated();
    }
    void rollbackInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        readWriteVersion.reset();
        resetToNotUpdated();
    }

    void addTableStatistic(catalog::TableCatalogEntry* tableEntry) {
        setToUpdated();
        initTableStatisticsForWriteTrx();
        readWriteVersion->tableStatisticPerTable[tableEntry->getTableID()] =
            constructTableStatistic(tableEntry);
    }
    void removeTableStatistic(common::table_id_t tableID) {
        setToUpdated();
        readOnlyVersion->tableStatisticPerTable.erase(tableID);
    }

    uint64_t getNumTuplesForTable(transaction::Transaction* transaction,
        common::table_id_t tableID) {
        if (transaction->isWriteTransaction()) {
            initTableStatisticsForWriteTrx();
            KU_ASSERT(readWriteVersion->tableStatisticPerTable.contains(tableID));
            return readWriteVersion->tableStatisticPerTable.at(tableID)->getNumTuples();
        }
        KU_ASSERT(readOnlyVersion->tableStatisticPerTable.contains(tableID));
        return readOnlyVersion->tableStatisticPerTable.at(tableID)->getNumTuples();
    }

    PropertyStatistics& getPropertyStatisticsForTable(const transaction::Transaction& transaction,
        common::table_id_t tableID, common::property_id_t propertyID);

    void setPropertyStatisticsForTable(common::table_id_t tableID, common::property_id_t propertyID,
        PropertyStatistics stats);

    static std::unique_ptr<MetadataDAHInfo> createMetadataDAHInfo(
        const common::LogicalType& dataType, BMFileHandle& metadataFH, BufferManager* bm, WAL* wal);

    void initTableStatisticsForWriteTrx();

protected:
    virtual std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableCatalogEntry* tableEntry) = 0;

    virtual std::string getTableStatisticsFilePath(const std::string& directory,
        common::FileVersionType dbFileType, common::VirtualFileSystem* fs) = 0;

    const TablesStatisticsContent* getVersion(transaction::TransactionType type) const {
        return type == transaction::TransactionType::READ_ONLY ? readOnlyVersion.get() :
                                                                 readWriteVersion.get();
    }

    void readFromFile(common::VirtualFileSystem* fs);
    void readFromFile(common::FileVersionType dbFileType, common::VirtualFileSystem* fs);

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType,
        transaction::TransactionType transactionType, common::VirtualFileSystem* fs);

    void initTableStatisticsForWriteTrxNoLock();

    void setToUpdated() { isUpdated = true; }
    void resetToNotUpdated() { isUpdated = false; }

protected:
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    bool isUpdated;
    std::unique_ptr<TablesStatisticsContent> readOnlyVersion;
    std::unique_ptr<TablesStatisticsContent> readWriteVersion;
    std::mutex mtx;
};

} // namespace storage
} // namespace kuzu
