#pragma once

#include "catalog/catalog.h"
#include "common/file_utils.h"
#include "rels_statistics.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const catalog::Catalog& catalog, MemoryManager& memoryManager, WAL* wal);

    inline Column* getRelPropertyColumn(common::RelDataDirection relDirection,
        common::table_id_t relTableID, uint64_t propertyIdx) const {
        return relTables.at(relTableID)->getPropertyColumn(relDirection, propertyIdx);
    }
    inline Lists* getRelPropertyLists(common::RelDataDirection relDirection,
        common::table_id_t relTableID, uint64_t propertyIdx) const {
        return relTables.at(relTableID)->getPropertyLists(relDirection, propertyIdx);
    }
    inline Column* getAdjColumn(
        common::RelDataDirection relDirection, common::table_id_t relTableID) const {
        return relTables.at(relTableID)->getAdjColumn(relDirection);
    }
    inline AdjLists* getAdjLists(
        common::RelDataDirection relDirection, common::table_id_t relTableID) const {
        return relTables.at(relTableID)->getAdjLists(relDirection);
    }

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of relTables. We just need to add the actual relTable to
    // relStore when checkpointing and not in recovery mode. In other words, this function should
    // only be called by wal_replayer during checkpointing, during which time no other transaction
    // is running on the system, so we can directly create and insert a RelTable into relTables.
    inline void createRelTable(
        common::table_id_t tableID, catalog::Catalog* catalog, MemoryManager* memoryManager) {
        relTables[tableID] = std::make_unique<RelTable>(*catalog, tableID, *memoryManager, wal);
    }

    // This function is used for testing only.
    inline uint32_t getNumRelTables() const { return relTables.size(); }

    inline RelTable* getRelTable(common::table_id_t tableID) const {
        return relTables.at(tableID).get();
    }

    inline RelsStatistics& getRelsStatistics() { return relsStatistics; }

    inline void removeRelTable(common::table_id_t tableID) {
        relTables.erase(tableID);
        relsStatistics.removeTableStatistic(tableID);
    }

    inline void prepareCommit() {
        if (relsStatistics.hasUpdates()) {
            wal->logTableStatisticsRecord(false /* isNodeTable */);
            relsStatistics.writeTablesStatisticsFileForWALRecord(wal->getDirectory());
        }
        for (auto& [_, relTable] : relTables) {
            relTable->prepareCommit();
        }
    }
    inline void prepareRollback() {
        if (relsStatistics.hasUpdates()) {
            wal->logTableStatisticsRecord(false /* isNodeTable */);
        }
        for (auto& [_, relTable] : relTables) {
            relTable->prepareRollback();
        }
    }
    inline void checkpointInMemory(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedTableID : updatedTables) {
            relTables.at(updatedTableID)->checkpointInMemory();
        }
    }
    inline void rollback(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedTableID : updatedTables) {
            relTables.at(updatedTableID)->rollback();
        }
    }

    inline bool isSingleMultiplicityInDirection(
        common::RelDataDirection relDirection, common::table_id_t relTableID) const {
        return relTables.at(relTableID)->isSingleMultiplicityInDirection(relDirection);
    }

    std::pair<std::vector<AdjLists*>, std::vector<Column*>> getAdjListsAndColumns(
        common::table_id_t boundTableID) const;

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<RelTable>> relTables;
    RelsStatistics relsStatistics;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
