#pragma once

#include "storage/store/chunked_node_group.h"
#include "storage/store/group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

// TODO(Guodong): Rename to InMemChunkedNodeGroupCollection
class ChunkedNodeGroupCollection {
public:
    explicit ChunkedNodeGroupCollection(std::vector<common::LogicalType> types)
        : types{std::move(types)} {}

    static std::pair<uint64_t, common::offset_t> getChunkIdxAndOffsetInChunk(
        common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY,
            rowIdx % ChunkedNodeGroup::CHUNK_CAPACITY);
    }

    const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() {
        return chunkedGroups;
    }
    ChunkedNodeGroup& getChunkedGroup(common::node_group_idx_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return *chunkedGroups[groupIdx];
    }
    ChunkedNodeGroup* getChunkedGroupUnsafe(common::node_group_idx_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }

    void append(const std::vector<common::ValueVector*>& vectors,
        common::offset_t startRowInVectors, common::row_idx_t numRowsToAppend);

    void merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(ChunkedNodeGroupCollection& other);

    uint64_t getNumChunkedGroups() const { return chunkedGroups.size(); }
    void clear() { chunkedGroups.clear(); }

private:
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
