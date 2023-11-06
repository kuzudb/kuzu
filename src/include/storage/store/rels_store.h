#pragma once

#include "catalog/catalog.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): NodesStore, RelsStore and StorageManager should be merged together.
class RelsStore {
public:
    RelsStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, const catalog::Catalog& catalog,
        BufferManager& bufferManager, WAL* wal, bool enableCompression);

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of relTables. We just need to add the actual relTable to
    // relStore when checkpointing and not in recovery mode. In other words, this function should
    // only be called by wal_replayer during checkpointing, during which time no other transaction
    // is running on the system, so we can directly create and insert a RelTable into relTables.
    inline void createRelTable(
        common::table_id_t tableID, catalog::Catalog* catalog, BufferManager* bufferManager) {
        relTables[tableID] =
            std::make_unique<RelTable>(dataFH, metadataFH, relsStatistics.get(), bufferManager,
                reinterpret_cast<catalog::RelTableSchema*>(
                    catalog->getReadOnlyVersion()->getTableSchema(tableID)),
                wal, enableCompression);
    }

    // This function is used for testing only.
    inline uint32_t getNumRelTables() const { return relTables.size(); }

    inline RelTable* getRelTable(common::table_id_t tableID) const {
        return relTables.at(tableID).get();
    }

    inline RelsStoreStats* getRelsStatistics() { return relsStatistics.get(); }

    inline void removeRelTable(common::table_id_t tableID) {
        relTables.erase(tableID);
        relsStatistics->removeTableStatistic(tableID);
    }

    inline void prepareCommit() {
        if (relsStatistics->hasUpdates()) {
            wal->logTableStatisticsRecord(false /* isNodeTable */);
            relsStatistics->writeTablesStatisticsFileForWALRecord(wal->getDirectory());
        }
    }
    inline void prepareRollback() {
        if (relsStatistics->hasUpdates()) {
            wal->logTableStatisticsRecord(false /* isNodeTable */);
        }
    }
    inline void checkpointInMemory(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedTableID : updatedTables) {
            relTables.at(updatedTableID)->checkpointInMemory();
        }
    }
    inline void rollbackInMemory(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedTableID : updatedTables) {
            relTables.at(updatedTableID)->rollbackInMemory();
        }
    }

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<RelTable>> relTables;
    std::unique_ptr<RelsStoreStats> relsStatistics;
    WAL* wal;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
