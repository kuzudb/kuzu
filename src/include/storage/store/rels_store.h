#pragma once

#include "catalog/catalog.h"
#include "common/file_utils.h"
#include "rels_statistics.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const Catalog& catalog, BufferManager& bufferManager, MemoryManager& memoryManager,
        bool isInMemoryMode, WAL* wal);

    // TODO(Ziyi): other getters requires (direction, nodeID, relID) but this one is requiring
    // (direction, relID, nodeID).
    inline Column* getRelPropertyColumn(RelDirection relDirection, table_id_t relTableID,
        table_id_t boundNodeTableID, uint64_t propertyIdx) const {
        return relTables.at(relTableID)
            ->getPropertyColumn(relDirection, boundNodeTableID, propertyIdx);
    }
    inline Lists* getRelPropertyLists(RelDirection relDirection, table_id_t boundNodeTableID,
        table_id_t relTableID, uint64_t propertyIdx) const {
        return relTables.at(relTableID)
            ->getPropertyLists(relDirection, boundNodeTableID, propertyIdx);
    }
    inline bool hasAdjColumn(
        RelDirection relDirection, table_id_t boundNodeTableID, table_id_t relTableID) const {
        return relTables.at(relTableID)->hasAdjColumn(relDirection, boundNodeTableID);
    }
    inline AdjColumn* getAdjColumn(
        RelDirection relDirection, table_id_t boundNodeTableID, table_id_t relTableID) const {
        return relTables.at(relTableID)->getAdjColumn(relDirection, boundNodeTableID);
    }
    inline bool hasAdjList(
        RelDirection relDirection, table_id_t boundNodeTableID, table_id_t relTableID) const {
        return relTables.at(relTableID)->hasAdjList(relDirection, boundNodeTableID);
    }
    inline AdjLists* getAdjLists(
        RelDirection relDirection, table_id_t boundNodeTableID, table_id_t relTableID) const {
        return relTables.at(relTableID)->getAdjLists(relDirection, boundNodeTableID);
    }

    pair<vector<AdjLists*>, vector<AdjColumn*>> getAdjListsAndColumns(
        const table_id_t tableID) const;

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of relTables. We just need to add the actual relTable to
    // relStore when checkpointing and not in recovery mode. In other words, this function should
    // only be called by wal_replayer during checkpointing, during which time no other transaction
    // is running on the system, so we can directly create and insert a RelTable into relTables.
    inline void createRelTable(table_id_t tableID, BufferManager* bufferManager, WAL* wal,
        Catalog* catalog, MemoryManager* memoryManager) {
        relTables[tableID] = make_unique<RelTable>(
            *catalog, tableID, *bufferManager, *memoryManager, isInMemoryMode, wal);
    }

    // This function is used for testing only.
    inline uint32_t getNumRelTables() const { return relTables.size(); }

    inline RelTable* getRelTable(table_id_t tableID) const { return relTables.at(tableID).get(); }

    inline RelsStatistics& getRelsStatistics() { return relsStatistics; }

    inline void removeRelTable(table_id_t tableID) {
        relTables.erase(tableID);
        relsStatistics.removeTableStatistic(tableID);
    }
    void prepareCommitOrRollbackIfNecessary(bool isCommit) {
        for (auto& [_, relTable] : relTables) {
            relTable->prepareCommitOrRollbackIfNecessary(isCommit);
        }
    }

private:
    unordered_map<table_id_t, unique_ptr<RelTable>> relTables;
    RelsStatistics relsStatistics;
    bool isInMemoryMode;
};

} // namespace storage
} // namespace kuzu
