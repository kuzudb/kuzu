#pragma once

#include <atomic>
#include <mutex>

#include "catalog/catalog_structs.h"
#include "common/ser_deser.h"
#include "spdlog/spdlog.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

using lock_t = unique_lock<mutex>;
using namespace transaction;
using namespace catalog;
typedef vector<atomic<uint64_t>> atomic_uint64_vec_t;

class TableStatistics {

public:
    TableStatistics() = default;

    virtual ~TableStatistics() = default;

    explicit TableStatistics(uint64_t numTuples) : numTuples{numTuples} {
        assert(numTuples != UINT64_MAX);
    }

    inline bool isEmpty() { return numTuples == 0; }

    inline uint64_t getNumTuples() const { return numTuples; }

    virtual inline void setNumTuples(uint64_t numTuples_) {
        assert(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

private:
    uint64_t numTuples;
};

struct TablesStatisticsContent {
    TablesStatisticsContent() : nextRelID{0} {}
    unordered_map<table_id_t, unique_ptr<TableStatistics>> tableStatisticPerTable;
    // This is only needed for RelsStatistics and is a temporary solution until we move to a
    // uniform node and edge ID scheme (and then open an issue about this.)
    uint64_t nextRelID;
};

class TablesStatistics {

public:
    TablesStatistics();

    virtual ~TablesStatistics() = default;

    inline void writeTablesStatisticsFileForWALRecord(const string& directory) {
        saveToFile(directory, DBFileType::WAL_VERSION, TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tablesStatisticsContentForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        tablesStatisticsContentForReadOnlyTrx = move(tablesStatisticsContentForWriteTrx);
    }

    inline TablesStatisticsContent* getReadOnlyVersion() const {
        return tablesStatisticsContentForReadOnlyTrx.get();
    }

    inline void addTableStatistic(TableSchema* tableSchema) {
        initTableStatisticPerTableForWriteTrxIfNecessary();
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
            constructTableStatistic(tableSchema);
    }
    inline void removeTableStatistic(table_id_t tableID) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.erase(tableID);
    }

protected:
    virtual inline string getTableTypeForPrinting() const = 0;

    virtual inline unique_ptr<TableStatistics> constructTableStatistic(
        TableSchema* tableSchema) = 0;

    virtual inline unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) = 0;

    virtual inline string getTableStatisticsFilePath(
        const string& directory, DBFileType dbFileType) = 0;

    virtual unique_ptr<TableStatistics> deserializeTableStatistics(
        uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) = 0;

    virtual void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) = 0;

    void readFromFile(const string& directory);

    void saveToFile(
        const string& directory, DBFileType dbFileType, TransactionType transactionType);

    void initTableStatisticPerTableForWriteTrxIfNecessary();

protected:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<TablesStatisticsContent> tablesStatisticsContentForReadOnlyTrx;
    unique_ptr<TablesStatisticsContent> tablesStatisticsContentForWriteTrx;
    mutex mtx;
};

} // namespace storage
} // namespace kuzu
