#pragma once

#include "storage/store/chunked_node_group.h"
#include "storage/store/group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class ChunkedNodeGroupCollection {
public:
    ChunkedNodeGroupCollection() : residencyState{ResidencyState::IN_MEMORY} {}
    ChunkedNodeGroupCollection(ResidencyState residencyState,
        std::vector<common::LogicalType> types)
        : residencyState{residencyState}, types{std::move(types)} {}
    explicit ChunkedNodeGroupCollection(std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup)
        : residencyState{ResidencyState::ON_DISK} {
        const auto lock = chunkedGroups.lock();
        chunkedGroups.appendGroup(lock, std::move(chunkedNodeGroup));
    }

    static std::pair<uint64_t, common::offset_t> getChunkIdxAndOffsetInChunk(
        common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY,
            rowIdx % ChunkedNodeGroup::CHUNK_CAPACITY);
    }

    const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() {
        const auto lock = chunkedGroups.lock();
        return chunkedGroups.getAllGroups(lock);
    }
    ChunkedNodeGroup& getChunkedGroup(common::node_group_idx_t groupIdx) {
        const auto lock = chunkedGroups.lock();
        const auto chunkedGroup = chunkedGroups.getGroup(lock, groupIdx);
        KU_ASSERT(chunkedGroup);
        return *chunkedGroup;
    }
    ChunkedNodeGroup& findChunkedGroupFromOffset(common::offset_t offset);

    void setChunkedGroup(common::node_group_idx_t groupIdx,
        std::unique_ptr<ChunkedNodeGroup> group) {
        const auto lock = chunkedGroups.lock();
        chunkedGroups.resize(lock, groupIdx + 1);
        chunkedGroups.replaceGroup(lock, groupIdx, std::move(group));
    }

    // Return num of rows before append.
    common::row_idx_t append(transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors, common::row_idx_t startRowInVectors,
        common::row_idx_t numRowsToAppend);
    common::row_idx_t append(transaction::Transaction* transaction,
        const ChunkedNodeGroup& chunkedGroup, common::row_idx_t numRowsToAppend);

    // `merge` are directly moving the chunkedGroup to the collection.
    void merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(std::unique_ptr<ChunkedNodeGroupCollection> other);

    uint64_t getNumChunkedGroups() {
        const auto lock = chunkedGroups.lock();
        return chunkedGroups.getNumGroups(lock);
    }
    void clear() {
        const auto lock = chunkedGroups.lock();
        chunkedGroups.clear(lock);
    }
    // TODO(Guodong): Should just keep an atomic counter instead of dynamically calculating it.
    common::row_idx_t getNumRows(const common::UniqLock& lock);

private:
    ResidencyState residencyState;
    std::vector<common::LogicalType> types;
    GroupCollection<ChunkedNodeGroup> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
