#pragma once

#include <memory>
#include <vector>

#include "nodes_statistics_and_deleted_ids.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace storage {

class NodesStore {

public:
    NodesStore(const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal);

    inline Column* getNodePropertyColumn(common::table_id_t tableID, uint64_t propertyIdx) const {
        return nodeTables.at(tableID)->getPropertyColumn(propertyIdx);
    }
    inline PrimaryKeyIndex* getPKIndex(common::table_id_t tableID) {
        return nodeTables[tableID]->getPKIndex();
    }
    inline NodesStatisticsAndDeletedIDs& getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs;
    }
    inline NodeTable* getNodeTable(common::table_id_t tableID) const {
        return nodeTables.at(tableID).get();
    }
    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of nodeTables. We just need to add the actual nodeTable to
    // nodeStore when checkpointing and not in recovery mode.
    inline void createNodeTable(
        common::table_id_t tableID, BufferManager* bufferManager, catalog::Catalog* catalog) {
        nodeTables[tableID] = std::make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs,
            *bufferManager, wal, catalog->getReadOnlyVersion()->getNodeTableSchema(tableID));
    }
    inline void removeNodeTable(common::table_id_t tableID) {
        nodeTables.erase(tableID);
        nodesStatisticsAndDeletedIDs.removeTableStatistic(tableID);
    }
    inline void prepareCommit() {
        if (nodesStatisticsAndDeletedIDs.hasUpdates()) {
            wal->logTableStatisticsRecord(true /* isNodeTable */);
            nodesStatisticsAndDeletedIDs.writeTablesStatisticsFileForWALRecord(wal->getDirectory());
        }
        for (auto& [_, nodeTable] : nodeTables) {
            nodeTable->prepareCommit();
        }
    }
    inline void prepareRollback() {
        if (nodesStatisticsAndDeletedIDs.hasUpdates()) {
            wal->logTableStatisticsRecord(true /* isNodeTable */);
        }
        for (auto& [_, nodeTable] : nodeTables) {
            nodeTable->prepareRollback();
        }
    }
    inline void checkpointInMemory(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedNodeTable : updatedTables) {
            nodeTables.at(updatedNodeTable)->checkpointInMemory();
        }
    }
    inline void rollback(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedNodeTable : updatedTables) {
            nodeTables.at(updatedNodeTable)->rollback();
        }
    }

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<NodeTable>> nodeTables;
    NodesStatisticsAndDeletedIDs nodesStatisticsAndDeletedIDs;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
