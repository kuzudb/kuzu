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
    RelStatistics(uint64_t numRels,
        std::vector<std::unordered_map<common::table_id_t, uint64_t>> numRelsPerDirectionBoundTable,
        common::offset_t nextRelOffset)
        : TableStatistics{numRels}, numRelsPerDirectionBoundTable{std::move(
                                        numRelsPerDirectionBoundTable)},
          nextRelOffset{nextRelOffset} {}
    explicit RelStatistics(
        std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs);

    inline uint64_t getNumRelsForDirectionBoundTable(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) const {
        if (!numRelsPerDirectionBoundTable[relDirection].contains(boundNodeTableID)) {
            return 0;
        }
        return numRelsPerDirectionBoundTable[relDirection].at(boundNodeTableID);
    }

    inline void setNumRelsForDirectionBoundTable(
        common::RelDirection relDirection, common::table_id_t boundTableID, uint64_t numRels) {
        numRelsPerDirectionBoundTable[relDirection][boundTableID] = numRels;
    }

    inline common::offset_t getNextRelOffset() const { return nextRelOffset; }

private:
    std::vector<std::unordered_map<common::table_id_t, uint64_t>> numRelsPerDirectionBoundTable;
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

    void setNumRelsForTable(common::table_id_t relTableID, uint64_t numRels);

    void assertNumRelsIsSound(
        std::unordered_map<common::table_id_t, uint64_t>& relsPerBoundTable, uint64_t numRels);

    void updateNumRelsByValue(common::table_id_t relTableID, common::table_id_t srcTableID,
        common::table_id_t dstTableID, int64_t value);

    // Note: This function will not set the numTuples field. That should be called separately.
    void setNumRelsPerDirectionBoundTableID(common::table_id_t tableID,
        std::vector<std::map<common::table_id_t, std::atomic<uint64_t>>>& directionNumRelsPerTable);

    common::offset_t getNextRelOffset(
        transaction::Transaction* transaction, common::table_id_t tableID);

protected:
    inline std::string getTableTypeForPrinting() const override { return "RelsStatistics"; }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) override {
        return std::make_unique<RelStatistics>(
            ((catalog::RelTableSchema*)tableSchema)->getSrcDstTableIDs());
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
