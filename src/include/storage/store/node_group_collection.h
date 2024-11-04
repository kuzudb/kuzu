#pragma once

#include "storage/enums/csr_node_group_scan_source.h"
#include "storage/stats/table_stats.h"
#include "storage/store/group_collection.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

using append_to_undo_buffer_func_t =
    std::function<void(const transaction::Transaction*, NodeGroup*, common::row_idx_t)>;

class NodeGroupCollection {
public:
    NodeGroupCollection(MemoryManager& memoryManager, const std::vector<common::LogicalType>& types,
        bool enableCompression, FileHandle* dataFH = nullptr, common::Deserializer* deSer = nullptr,
        append_to_undo_buffer_func_t appendToUndoBufferFunc = defaultAppendToUndoBuffer);

    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors);
    void append(const transaction::Transaction* transaction, NodeGroupCollection& other);
    void append(const transaction::Transaction* transaction, NodeGroup& nodeGroup);

    // This function only tries to append data into the last node group, and if the last node group
    // is not enough to hold all the data, it will append partially and return the number of rows
    // appended.
    // The returned values are the startOffset and numValuesAppended.
    // NOTE: This is specially coded to only be used by NodeBatchInsert for now.
    std::pair<common::offset_t, common::offset_t> appendToLastNodeGroupAndFlushWhenFull(
        transaction::Transaction* transaction, ChunkedNodeGroup& chunkedGroup);

    common::row_idx_t getNumTotalRows();
    common::node_group_idx_t getNumNodeGroups() {
        const auto lock = nodeGroups.lock();
        return nodeGroups.getNumGroups(lock);
    }
    NodeGroup* getNodeGroupNoLock(const common::node_group_idx_t groupIdx) {
        KU_ASSERT(nodeGroups.getGroupNoLock(groupIdx)->getNodeGroupIdx() == groupIdx);
        return nodeGroups.getGroupNoLock(groupIdx);
    }
    NodeGroup* getNodeGroup(const common::node_group_idx_t groupIdx,
        bool mayOutOfBound = false) const {
        const auto lock = nodeGroups.lock();
        if (mayOutOfBound && groupIdx >= nodeGroups.getNumGroups(lock)) {
            return nullptr;
        }
        KU_ASSERT(nodeGroups.getGroupNoLock(groupIdx)->getNodeGroupIdx() == groupIdx);
        return nodeGroups.getGroup(lock, groupIdx);
    }
    NodeGroup* getOrCreateNodeGroup(common::node_group_idx_t groupIdx, NodeGroupDataFormat format);

    void setNodeGroup(const common::node_group_idx_t nodeGroupIdx,
        std::unique_ptr<NodeGroup> group) {
        const auto lock = nodeGroups.lock();
        nodeGroups.replaceGroup(lock, nodeGroupIdx, std::move(group));
    }

    void commitInsert(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx, common::transaction_t commitTS,
        CSRNodeGroupScanSource source = CSRNodeGroupScanSource::NONE);
    void commitDelete(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx, common::transaction_t commitTS,
        CSRNodeGroupScanSource source = CSRNodeGroupScanSource::NONE);

    void rollbackInsert(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx,
        CSRNodeGroupScanSource source = CSRNodeGroupScanSource::NONE);
    void rollbackDelete(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx,
        CSRNodeGroupScanSource source = CSRNodeGroupScanSource::NONE);

    void clear() {
        const auto lock = nodeGroups.lock();
        nodeGroups.clear(lock);
    }

    common::column_id_t getNumColumns() const { return types.size(); }

    void addColumn(transaction::Transaction* transaction, TableAddColumnState& addColumnState);

    uint64_t getEstimatedMemoryUsage();

    void checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state);

    TableStats getStats() const { return stats.copy(); }

    void serialize(common::Serializer& ser);
    void deserialize(common::Deserializer& deSer, MemoryManager& memoryManager);

private:
    static void defaultAppendToUndoBuffer(const transaction::Transaction*, NodeGroup*,
        common::row_idx_t);

    bool enableCompression;
    // Num rows in the collection regardless of deletions.
    common::row_idx_t numTotalRows;
    std::vector<common::LogicalType> types;
    GroupCollection<NodeGroup> nodeGroups;
    FileHandle* dataFH;
    TableStats stats;
    append_to_undo_buffer_func_t appendToUndoBufferFunc;
};

} // namespace storage
} // namespace kuzu
