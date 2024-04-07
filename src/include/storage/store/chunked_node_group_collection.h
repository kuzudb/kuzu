#pragma once

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroupCollection {
public:
    static constexpr uint64_t CHUNK_CAPACITY = 2048;

    explicit ChunkedNodeGroupCollection(std::vector<common::LogicalType> types)
        : types{std::move(types)} {}
    DELETE_COPY_DEFAULT_MOVE(ChunkedNodeGroupCollection);

    static std::pair<uint64_t, common::offset_t> getChunkIdxAndOffsetInChunk(
        common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / CHUNK_CAPACITY, rowIdx % CHUNK_CAPACITY);
    }

    inline const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() const {
        return chunkedGroups;
    }
    inline const ChunkedNodeGroup* getChunkedGroup(common::node_group_idx_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline ChunkedNodeGroup* getChunkedGroupUnsafe(common::node_group_idx_t groupIdx) {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }

    void append(const std::vector<common::ValueVector*>& vectors,
        const common::SelectionVector& selVector);

    void merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(ChunkedNodeGroupCollection& other);

    inline uint64_t getNumChunkedGroups() const { return chunkedGroups.size(); }
    inline void clear() { chunkedGroups.clear(); }

private:
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
