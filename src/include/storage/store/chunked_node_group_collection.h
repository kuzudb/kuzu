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

    inline const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() const {
        return chunkedGroups;
    }
    inline const ChunkedNodeGroup* getChunkedGroup(uint64_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline ChunkedNodeGroup* getChunkedGroupUnsafe(uint64_t groupIdx) {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline uint64_t getNumChunks() const { return chunkedGroups.size(); }

    void append(
        const std::vector<common::ValueVector*>& vectors, const common::SelectionVector& selVector);
    void append(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(ChunkedNodeGroupCollection& chunkedGroupCollection);

private:
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
