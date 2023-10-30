#pragma once

#include <memory>
#include <vector>

#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace storage {

class NodesStore {
public:
    NodesStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::AccessMode accessMode,
        const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal,
        bool enableCompression);

    inline PrimaryKeyIndex* getPKIndex(common::table_id_t tableID) {
        return nodeTables[tableID]->getPKIndex();
    }
    inline NodesStoreStatsAndDeletedIDs* getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs.get();
    }
    inline NodeTable* getNodeTable(common::table_id_t tableID) const {
        return nodeTables.at(tableID).get();
    }
    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of nodeTables. We just need to add the actual nodeTable to
    // nodeStore when checkpointing and not in recovery mode.
    inline void createNodeTable(
        common::table_id_t tableID, BufferManager* bufferManager, catalog::Catalog* catalog) {
        nodeTables[tableID] = std::make_unique<NodeTable>(dataFH, metadataFH,
            common::AccessMode::READ_WRITE, nodesStatisticsAndDeletedIDs.get(), *bufferManager, wal,
            reinterpret_cast<catalog::NodeTableSchema*>(
                catalog->getReadOnlyVersion()->getTableSchema(tableID)),
            enableCompression);
    }
    inline void removeNodeTable(common::table_id_t tableID) {
        nodeTables.erase(tableID);
        nodesStatisticsAndDeletedIDs->removeTableStatistic(tableID);
    }
    inline void prepareCommit() {
        if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
            wal->logTableStatisticsRecord(true /* isNodeTable */);
            nodesStatisticsAndDeletedIDs->writeTablesStatisticsFileForWALRecord(
                wal->getDirectory());
        }
        for (auto& [_, nodeTable] : nodeTables) {
            nodeTable->prepareCommit();
        }
    }
    inline void prepareRollback() {
        if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
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
    inline void rollbackInMemory(const std::unordered_set<common::table_id_t>& updatedTables) {
        for (auto updatedNodeTable : updatedTables) {
            nodeTables.at(updatedNodeTable)->rollbackInMemory();
        }
    }

private:
    std::map<common::table_id_t, std::unique_ptr<NodeTable>> nodeTables;
    std::unique_ptr<NodesStoreStatsAndDeletedIDs> nodesStatisticsAndDeletedIDs;
    WAL* wal;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
