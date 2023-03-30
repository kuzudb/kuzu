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
    RelStatistics() : TableStatistics{0 /* numTuples */}, nextRelOffset{0} {}
    RelStatistics(uint64_t numRels, common::offset_t nextRelOffset)
        : TableStatistics{numRels}, nextRelOffset{nextRelOffset} {}

    inline common::offset_t getNextRelOffset() const { return nextRelOffset; }

private:
    common::offset_t nextRelOffset;
};

// Manages the disk image of the numRels and numRelsPerDirectionBoundTable.
class RelsStatistics : public TablesStatistics {

public:
    // Should only be used by saveInitialRelsStatisticsToFile to start a database from an empty
    // directory.
    RelsStatistics() : TablesStatistics{} {};
    // Should be used when an already loaded database is started from a directory.
    explicit RelsStatistics(const std::string& directory) : TablesStatistics{} {
        logger->info("Initializing {}.", "RelsStatistics");
        readFromFile(directory);
        logger->info("Initialized {}.", "RelsStatistics");
    }

    // Should only be used by tests.
    explicit RelsStatistics(std::unordered_map<common::table_id_t, std::unique_ptr<RelStatistics>>
            relStatisticPerTable_);

    static inline void saveInitialRelsStatisticsToFile(const std::string& directory) {
        std::make_unique<RelsStatistics>()->saveToFile(
            directory, common::DBFileType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    inline RelStatistics* getRelStatistics(common::table_id_t tableID) const {
        auto& tableStatisticPerTable =
            tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable;
        return (RelStatistics*)tableStatisticPerTable[tableID].get();
    }

    void setNumTuplesForTable(common::table_id_t relTableID, uint64_t numRels) override;

    void updateNumRelsByValue(common::table_id_t relTableID, int64_t value);

    common::offset_t getNextRelOffset(
        transaction::Transaction* transaction, common::table_id_t tableID);

protected:
    inline std::string getTableTypeForPrinting() const override { return "RelsStatistics"; }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) override {
        return std::make_unique<RelStatistics>();
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<RelStatistics>(*(RelStatistics*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) override {
        return StorageUtils::getRelsStatisticsFilePath(directory, dbFileType);
    }

    inline void increaseNextRelOffset(common::table_id_t relTableID, uint64_t numTuples) {
        ((RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(relTableID)
                .get())
            ->nextRelOffset += numTuples;
    }

    std::unique_ptr<TableStatistics> deserializeTableStatistics(uint64_t numTuples,
        uint64_t& offset, common::FileInfo* fileInfo, uint64_t tableID) override;

    void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, common::FileInfo* fileInfo) override;
};

} // namespace storage
} // namespace kuzu
