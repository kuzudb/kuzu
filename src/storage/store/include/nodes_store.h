#pragma once

#include <memory>
#include <vector>

#include "nodes_statistics_and_deleted_ids.h"

#include "src/storage/store/include/node_table.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

class NodesStore {

public:
    NodesStore(const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

    inline Column* getNodePropertyColumn(table_id_t tableID, uint64_t propertyIdx) const {
        return nodeTables[tableID]->getPropertyColumn(propertyIdx);
    }
    inline UnstructuredPropertyLists* getNodeUnstrPropertyLists(table_id_t tableID) const {
        return nodeTables[tableID]->getUnstrPropertyLists();
    }
    inline HashIndex* getIDIndex(table_id_t tableID) { return nodeTables[tableID]->getIDIndex(); }

    inline NodesStatisticsAndDeletedIDs& getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs;
    }

    // TODO: rename to getNodeTable
    inline NodeTable* getNode(table_id_t tableID) const { return nodeTables[tableID].get(); }

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of nodeTables. We just need to add the actual nodeTable to
    // nodeStore when checkpointing and not in recovery mode.
    inline void createNodeTable(
        table_id_t tableID, BufferManager* bufferManager, WAL* wal, Catalog* catalog) {
        nodeTables.push_back(make_unique<NodeTable>(&nodesStatisticsAndDeletedIDs, *bufferManager,
            isInMemoryMode, wal, catalog->getReadOnlyVersion()->getNodeTableSchema(tableID)));
    }

    void prepareUnstructuredPropertyListsToCommitOrRollbackIfNecessary(bool isCommit) {
        for (uint64_t i = 0; i < nodeTables.size(); ++i) {
            nodeTables[i]->getUnstrPropertyLists()->prepareCommitOrRollbackIfNecessary(isCommit);
        }
    }

private:
    vector<unique_ptr<NodeTable>> nodeTables;
    NodesStatisticsAndDeletedIDs nodesStatisticsAndDeletedIDs;
    // Used to dynamically create nodeTables during checkpointing.
    bool isInMemoryMode;
};

} // namespace storage
} // namespace graphflow
