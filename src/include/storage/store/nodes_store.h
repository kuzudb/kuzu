#pragma once

#include <memory>
#include <vector>

#include "nodes_statistics_and_deleted_ids.h"
#include "storage/store/node_table.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

class NodesStore {

public:
    NodesStore(const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

    inline Column* getNodePropertyColumn(table_id_t tableID, uint64_t propertyIdx) const {
        return nodeTables.at(tableID)->getPropertyColumn(propertyIdx);
    }
    inline PrimaryKeyIndex* getPKIndex(table_id_t tableID) {
        return nodeTables[tableID]->getPKIndex();
    }
    inline NodesStatisticsAndDeletedIDs& getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs;
    }
    inline NodeTable* getNodeTable(table_id_t tableID) const {
        return nodeTables.at(tableID).get();
    }
    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of nodeTables. We just need to add the actual nodeTable to
    // nodeStore when checkpointing and not in recovery mode.
    inline void createNodeTable(
        table_id_t tableID, BufferManager* bufferManager, WAL* wal, Catalog* catalog) {
        nodeTables[tableID] = make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs, *bufferManager,
            isInMemoryMode, wal, catalog->getReadOnlyVersion()->getNodeTableSchema(tableID));
    }
    inline void removeNodeTable(table_id_t tableID) {
        nodeTables.erase(tableID);
        nodesStatisticsAndDeletedIDs.removeTableStatistic(tableID);
    }
    inline void prepareCommitOrRollbackIfNecessary(bool isCommit) {
        for (auto& [_, nodeTable] : nodeTables) {
            nodeTable->prepareCommitOrRollbackIfNecessary(isCommit);
        }
    }

private:
    unordered_map<table_id_t, unique_ptr<NodeTable>> nodeTables;
    NodesStatisticsAndDeletedIDs nodesStatisticsAndDeletedIDs;
    // Used to dynamically create nodeTables during checkpointing.
    bool isInMemoryMode;
};

} // namespace storage
} // namespace kuzu
