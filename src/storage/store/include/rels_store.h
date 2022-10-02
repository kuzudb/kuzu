#pragma once

#include "rels_statistics.h"

#include "src/catalog/include/catalog.h"
#include "src/common/include/file_utils.h"
#include "src/storage/store/include/rel_table.h"

using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerTable,
        BufferManager& bufferManager, MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    inline Column* getRelPropertyColumn(const table_id_t& relTableID, const table_id_t& nodeTableID,
        const uint64_t& propertyIdx) const {
        return relTables.at(relTableID)->getPropertyColumn(nodeTableID, propertyIdx);
    }
    inline Lists* getRelPropertyLists(const RelDirection& relDirection,
        const table_id_t& nodeTableID, const table_id_t& relTableID,
        const uint64_t& propertyIdx) const {
        return relTables.at(relTableID)->getPropertyLists(relDirection, nodeTableID, propertyIdx);
    }
    inline AdjColumn* getAdjColumn(const RelDirection& relDirection, const table_id_t& nodeTableID,
        const table_id_t& relTableID) const {
        return relTables.at(relTableID)->getAdjColumn(relDirection, nodeTableID);
    }
    inline AdjLists* getAdjLists(const RelDirection& relDirection, const table_id_t& nodeTableID,
        const table_id_t& relTableID) const {
        return relTables.at(relTableID)->getAdjLists(relDirection, nodeTableID);
    }

    pair<vector<AdjLists*>, vector<AdjColumn*>> getAdjListsAndColumns(
        const table_id_t tableID) const;

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of relTables. We just need to add the actual relTable to
    // relStore when checkpointing and not in recovery mode. In other words, this function should
    // only be called by wal_replayer during checkpointing, during which time no other transaction
    // is running on the system, so we can directly create and insert a RelTable into relTables.
    inline void createRelTable(table_id_t tableID, vector<uint64_t>& maxNodeOffsetsPerTable,
        BufferManager* bufferManager, WAL* wal, Catalog* catalog, MemoryManager* memoryManager) {
        relTables[tableID] = make_unique<RelTable>(*catalog, maxNodeOffsetsPerTable, tableID,
            *bufferManager, *memoryManager, isInMemoryMode, wal);
    }

    // This function is used for testing only.
    inline uint32_t getNumRelTables() const { return relTables.size(); }

    inline RelTable* getRel(table_id_t tableID) const { return relTables.at(tableID).get(); }

    inline RelsStatistics& getRelsStatistics() { return relsStatistics; }

    inline void removeRelTable(table_id_t tableID) {
        relTables.erase(tableID);
        relsStatistics.removeTableStatistic(tableID);
    }
    void prepareAdjAndRelPropertyListsToCommitOrRollbackIfNecessary(bool isCommit) {
        for (auto& relTable : relTables) {
            relTable.second->prepareCommitOrRollbackIfNecessary(isCommit);
        }
    }

private:
    unordered_map<table_id_t, unique_ptr<RelTable>> relTables;
    RelsStatistics relsStatistics;
    bool isInMemoryMode;
};

} // namespace storage
} // namespace graphflow
