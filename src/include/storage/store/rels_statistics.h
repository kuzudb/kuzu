#pragma once

#include <map>

#include "storage/storage_utils.h"
#include "table_statistics.h"

namespace kuzu {
namespace storage {
class RelsStatistics;
class RelStatistics : public TableStatistics {
    friend class RelsStatistics;

public:
    RelStatistics(
        uint64_t numRels, vector<unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTable)
        : TableStatistics{numRels}, numRelsPerDirectionBoundTable{
                                        move(numRelsPerDirectionBoundTable)} {}
    RelStatistics(vector<pair<table_id_t, table_id_t>> srcDstTableIDs);

    inline uint64_t getNumRelsForDirectionBoundTable(
        RelDirection relDirection, table_id_t boundNodeTableID) const {
        if (!numRelsPerDirectionBoundTable[relDirection].contains(boundNodeTableID)) {
            return 0;
        }
        return numRelsPerDirectionBoundTable[relDirection].at(boundNodeTableID);
    }

    inline void setNumRelsForDirectionBoundTable(
        RelDirection relDirection, table_id_t boundTableID, uint64_t numRels) {
        numRelsPerDirectionBoundTable[relDirection][boundTableID] = numRels;
    }

private:
    vector<unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTable;
};

// Manages the disk image of the numRels and numRelsPerDirectionBoundTable.
class RelsStatistics : public TablesStatistics {

public:
    // Should only be used by saveInitialRelsStatisticsToFile to start a database from an empty
    // directory.
    RelsStatistics() : TablesStatistics{} {};
    // Should be used when an already loaded database is started from a directory.
    explicit RelsStatistics(const string& directory) : TablesStatistics{} {
        logger->info("Initializing {}.", "RelsStatistics");
        readFromFile(directory);
        logger->info("Initialized {}.", "RelsStatistics");
    }

    // Should only be used by tests.
    explicit RelsStatistics(
        unordered_map<table_id_t, unique_ptr<RelStatistics>> relStatisticPerTable_);

    static inline void saveInitialRelsStatisticsToFile(const string& directory) {
        make_unique<RelsStatistics>()->saveToFile(
            directory, DBFileType::ORIGINAL, TransactionType::READ_ONLY);
    }

    inline RelStatistics* getRelStatistics(table_id_t tableID) const {
        auto& tableStatisticPerTable =
            tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable;
        return (RelStatistics*)tableStatisticPerTable[tableID].get();
    }

    void setNumRelsForTable(table_id_t relTableID, uint64_t numRels);

    void assertNumRelsIsSound(
        unordered_map<table_id_t, uint64_t>& relsPerBoundTable, uint64_t numRels);

    void updateNumRelsByValue(
        table_id_t relTableID, table_id_t srcTableID, table_id_t dstTableID, int64_t value);

    // Note: This function will not set the numTuples field. That should be called separately.
    void setNumRelsPerDirectionBoundTableID(
        table_id_t tableID, vector<map<table_id_t, atomic<uint64_t>>>& directionNumRelsPerTable);

    uint64_t getNextRelID(Transaction* transaction);

protected:
    inline string getTableTypeForPrinting() const override { return "RelsStatistics"; }

    inline unique_ptr<TableStatistics> constructTableStatistic(TableSchema* tableSchema) override {
        return make_unique<RelStatistics>(((RelTableSchema*)tableSchema)->getSrcDstTableIDs());
    }

    inline unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return make_unique<RelStatistics>(*(RelStatistics*)tableStatistics);
    }

    inline string getTableStatisticsFilePath(
        const string& directory, DBFileType dbFileType) override {
        return StorageUtils::getRelsStatisticsFilePath(directory, dbFileType);
    }

    unique_ptr<TableStatistics> deserializeTableStatistics(
        uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) override;

    void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) override;
};

} // namespace storage
} // namespace kuzu
