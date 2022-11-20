#pragma once

#include "src/catalog/include/catalog.h"
#include "src/storage/index/include/hash_index.h"
#include "src/storage/storage_structure/include/lists/lists.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"
#include "src/storage/wal/include/wal.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class NodeTable {

public:
    NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
        BufferManager& bufferManager, bool isInMemory, WAL* wal, NodeTableSchema* nodeTableSchema);

    void loadColumnsAndListsFromDisk(
        NodeTableSchema* nodeTableSchema, BufferManager& bufferManager, WAL* wal);

    inline Column* getPropertyColumn(uint64_t propertyIdx) {
        return propertyColumns[propertyIdx].get();
    }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }

    inline NodesStatisticsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return nodesStatisticsAndDeletedIDs;
    }

    inline table_id_t getTableID() const { return tableID; }

    node_offset_t addNodeAndResetProperties(ValueVector* primaryKeyVector);
    void deleteNodes(ValueVector* nodeIDVector, ValueVector* primaryKeyVector);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);

    inline void checkpointInMemoryIfNecessary() { pkIndex->checkpointInMemoryIfNecessary(); }
    inline void rollbackInMemoryIfNecessary() { pkIndex->rollbackInMemoryIfNecessary(); }

private:
    void deleteNode(node_offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const;

public:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;

private:
    // This is for structured properties.
    vector<unique_ptr<Column>> propertyColumns;
    // The index for ID property.
    // TODO(Guodong): rename this to primary key index
    unique_ptr<PrimaryKeyIndex> pkIndex;
    table_id_t tableID;
    bool isInMemory;
};

} // namespace storage
} // namespace kuzu
