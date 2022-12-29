#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

class NodeTable {

public:
    NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
        BufferManager& bufferManager, bool isInMemory, WAL* wal, NodeTableSchema* nodeTableSchema);

    void initializeData(NodeTableSchema* nodeTableSchema, BufferManager& bufferManager, WAL* wal);

    inline node_offset_t getMaxNodeOffset(Transaction* trx) const {
        return nodesStatisticsAndDeletedIDs->getMaxNodeOffset(trx, tableID);
    }
    inline void setSelVectorForDeletedOffsets(
        Transaction* trx, shared_ptr<ValueVector>& vector) const {
        assert(vector->isSequential());
        nodesStatisticsAndDeletedIDs->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }

    void scan(Transaction* transaction, const shared_ptr<ValueVector>& inputIDVector,
        const vector<uint32_t>& columnIdxes, vector<shared_ptr<ValueVector>> outputVectors);

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

private:
    // TODO(Guodong): Consider moving statistics and deleted ids to catalog.
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    // This is for properties.
    vector<unique_ptr<Column>> propertyColumns;
    // The index for ID property.
    unique_ptr<PrimaryKeyIndex> pkIndex;
    table_id_t tableID;
    bool isInMemory;
};

} // namespace storage
} // namespace kuzu
