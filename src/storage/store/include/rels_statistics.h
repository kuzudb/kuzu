#pragma once

#include "table_statistics.h"

#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {
class RelsStatistics;
class RelStatistics : public TableStatistics {
    friend class RelsStatistics;

public:
    RelStatistics(
        uint64_t numRels, vector<unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTable)
        : TableStatistics{numRels}, numRelsPerDirectionBoundTable{
                                        move(numRelsPerDirectionBoundTable)} {}
    RelStatistics(SrcDstTableIDs srcDstTableIDs);

    inline uint64_t getNumRelsForDirectionBoundTable(
        RelDirection relDirection, table_id_t boundNodeTableID) const {
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

    inline void setNumRelsForTable(table_id_t tableID, uint64_t numRels) {
        lock_t lck{mtx};
        initTableStatisticPerTableForWriteTrxIfNecessary();
        assert(tableID < tableStatisticPerTableForWriteTrx->size());
        ((RelStatistics*)((*tableStatisticPerTableForWriteTrx)[tableID].get()))
            ->setNumTuples(numRels);
    }

    void setNumRelsPerDirectionBoundTableID(
        table_id_t tableID, vector<unique_ptr<atomic_uint64_vec_t>>& directionNumRelsPerTable);

    uint64_t getNextRelID();

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
} // namespace graphflow
