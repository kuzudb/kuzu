#pragma once

#include "storage/stats/table_statistics.h"

namespace kuzu {
namespace storage {

struct TablesStatisticsContent {
    std::unordered_map<common::table_id_t, std::unique_ptr<TableStatistics>> tableStatisticPerTable;
};

class WAL;
class TablesStatistics {
public:
    TablesStatistics(BMFileHandle* metadataFH);

    virtual ~TablesStatistics() = default;

    virtual void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) = 0;

    inline void writeTablesStatisticsFileForWALRecord(const std::string& directory) {
        saveToFile(directory, common::DBFileType::WAL_VERSION, transaction::TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tablesStatisticsContentForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        tablesStatisticsContentForReadOnlyTrx = std::move(tablesStatisticsContentForWriteTrx);
    }

    inline TablesStatisticsContent* getReadOnlyVersion() const {
        return tablesStatisticsContentForReadOnlyTrx.get();
    }

    inline void addTableStatistic(catalog::TableSchema* tableSchema) {
        initTableStatisticsForWriteTrx();
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
            constructTableStatistic(tableSchema);
    }
    inline void removeTableStatistic(common::table_id_t tableID) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.erase(tableID);
    }

    inline uint64_t getNumTuplesForTable(common::table_id_t tableID) {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID]
            ->getNumTuples();
    }

    PropertyStatistics& getPropertyStatisticsForTable(const transaction::Transaction& transaction,
        common::table_id_t tableID, common::property_id_t propertyID);

    void setPropertyStatisticsForTable(
        common::table_id_t tableID, common::property_id_t propertyID, PropertyStatistics stats);

    static std::unique_ptr<MetadataDAHInfo> createMetadataDAHInfo(
        const common::LogicalType& dataType, BMFileHandle& metadataFH, BufferManager* bm, WAL* wal);

protected:
    virtual std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) = 0;

    virtual std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) = 0;

    virtual std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) = 0;

    void readFromFile(const std::string& directory);
    void readFromFile(const std::string& directory, common::DBFileType dbFileType);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType,
        transaction::TransactionType transactionType);

    void initTableStatisticsForWriteTrx();
    void initTableStatisticsForWriteTrxNoLock();

protected:
    BMFileHandle* metadataFH;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForReadOnlyTrx;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForWriteTrx;
    std::mutex mtx;
};
} // namespace storage
} // namespace kuzu
