#pragma once

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class ChunkedNodeGroupCollection {
public:
    ChunkedNodeGroupCollection(ResidencyState residencyState,
        std::vector<common::LogicalType> types)
        : residencyState{residencyState}, types{std::move(types)} {}
    explicit ChunkedNodeGroupCollection(std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup)
        : residencyState{ResidencyState::ON_DISK} {
        chunkedGroups.push_back(std::move(chunkedNodeGroup));
    }
    DELETE_COPY_DEFAULT_MOVE(ChunkedNodeGroupCollection);

    static std::pair<uint64_t, common::offset_t> getChunkIdxAndOffsetInChunk(
        common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY,
            rowIdx % ChunkedNodeGroup::CHUNK_CAPACITY);
    }

    const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() const {
        return chunkedGroups;
    }
    const ChunkedNodeGroup& getChunkedGroup(common::node_group_idx_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return *chunkedGroups[groupIdx].get();
    }
    ChunkedNodeGroup& getChunkedGroup(common::node_group_idx_t groupIdx) {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return *chunkedGroups[groupIdx].get();
    }
    ChunkedNodeGroup& findChunkedGroupFromOffset(common::offset_t offset) const;

    void setChunkedGroup(common::node_group_idx_t groupIdx,
        std::unique_ptr<ChunkedNodeGroup> group) {
        chunkedGroups.resize(groupIdx + 1);
        chunkedGroups[groupIdx] = std::move(group);
    }

    // Return num of rows before append.
    common::row_idx_t append(const std::vector<common::ValueVector*>& vectors,
        const common::SelectionVector& selVector);
    common::row_idx_t append(const ChunkedNodeGroup& chunkedGroup);
    common::row_idx_t append(const ChunkedNodeGroupCollection& other,
        common::offset_t offsetInOtherCollection, common::offset_t numRowsToAppend);

    // `merge` are directly moving the chunkedGroup to the collection.
    void merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(ChunkedNodeGroupCollection& other);

    uint64_t getNumChunkedGroups() const { return chunkedGroups.size(); }
    void clear() { chunkedGroups.clear(); }
    common::row_idx_t getNumRows() const;

private:
    // TODO(Guodong): Should handle concurrency?
    ResidencyState residencyState;
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
