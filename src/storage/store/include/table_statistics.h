#pragma once

#include <atomic>

#include "spdlog/spdlog.h"

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/ser_deser.h"
#include "src/transaction/include/transaction.h"

namespace graphflow {
namespace storage {

using lock_t = unique_lock<mutex>;
using namespace transaction;
using namespace catalog;
typedef vector<atomic<uint64_t>> atomic_uint64_vec_t;

class TableStatistics {

public:
    TableStatistics() = default;
    TableStatistics(uint64_t numTuples) : numTuples{numTuples} {}

    inline uint64_t getNumTuples() const { return numTuples; }

    inline void setNumTuples(uint64_t numTuples_) { numTuples = numTuples_; }

private:
    uint64_t numTuples;
};

class TablesStatistics {

public:
    TablesStatistics();

    inline void writeTablesStatisticsFileForWALRecord(const string& directory) {
        saveToFile(directory, DBFileType::WAL_VERSION, TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tableStatisticPerTableForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        tableStatisticPerTableForReadOnlyTrx = move(tableStatisticPerTableForWriteTrx);
    }

    inline vector<unique_ptr<TableStatistics>>* getReadOnlyVersion() const {
        return tableStatisticPerTableForReadOnlyTrx.get();
    }

    inline void addTableStatistic(TableSchema* tableSchema) {
        initTableStatisticPerTableForWriteTrxIfNecessary();
        tableStatisticPerTableForWriteTrx->push_back(constructTableStatistic(tableSchema));
    }
    inline void deleteTableStatistic(table_id_t tableID) {
        tableStatisticPerTableForReadOnlyTrx->erase(
            tableStatisticPerTableForReadOnlyTrx->begin() + tableID);
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
    unique_ptr<vector<unique_ptr<TableStatistics>>> tableStatisticPerTableForReadOnlyTrx;
    unique_ptr<vector<unique_ptr<TableStatistics>>> tableStatisticPerTableForWriteTrx;
    mutex mtx;
};

} // namespace storage
} // namespace graphflow
