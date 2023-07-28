#pragma once

#include <atomic>
#include <mutex>

#include "catalog/table_schema.h"
#include "common/ser_deser.h"
#include "spdlog/spdlog.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

using lock_t = std::unique_lock<std::mutex>;
using atomic_uint64_vec_t = std::vector<std::atomic<uint64_t>>;

class TableStatistics {

public:
    TableStatistics() : numTuples{0} {}

    virtual ~TableStatistics() = default;

    explicit TableStatistics(uint64_t numTuples) : numTuples{numTuples} {
        assert(numTuples != UINT64_MAX);
    }

    inline bool isEmpty() const { return numTuples == 0; }

    inline uint64_t getNumTuples() const { return numTuples; }

    virtual inline void setNumTuples(uint64_t numTuples_) {
        assert(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

private:
    uint64_t numTuples;
};

struct TablesStatisticsContent {
    TablesStatisticsContent() = default;
    std::unordered_map<common::table_id_t, std::unique_ptr<TableStatistics>> tableStatisticPerTable;
};

class TablesStatistics {

public:
    TablesStatistics();

    virtual ~TablesStatistics() = default;

    virtual void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) = 0;

    inline void writeTablesStatisticsFileForWALRecord(const std::string& directory) {
        saveToFile(directory, common::DBFileType::WAL_VERSION, transaction::TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tablesStatisticsContentForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        tablesStatisticsContentForReadOnlyTrx = std::move(tablesStatisticsContentForWriteTrx);
    }

    inline TablesStatisticsContent* getReadOnlyVersion() const {
        return tablesStatisticsContentForReadOnlyTrx.get();
    }

    inline void addTableStatistic(catalog::TableSchema* tableSchema) {
        initTableStatisticPerTableForWriteTrxIfNecessary();
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

protected:
    virtual inline std::string getTableTypeForPrinting() const = 0;

    virtual inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) = 0;

    virtual inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) = 0;

    virtual inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) = 0;

    virtual std::unique_ptr<TableStatistics> deserializeTableStatistics(
        uint64_t numTuples, uint64_t& offset, common::FileInfo* fileInfo, uint64_t tableID) = 0;

    virtual void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, common::FileInfo* fileInfo) = 0;

    void readFromFile(const std::string& directory);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType,
        transaction::TransactionType transactionType);

    void initTableStatisticPerTableForWriteTrxIfNecessary();

protected:
    std::shared_ptr<spdlog::logger> logger;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForReadOnlyTrx;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForWriteTrx;
    std::mutex mtx;
};

} // namespace storage
} // namespace kuzu
