#pragma once

#include "common/api.h"
#include "common/types/types.h"
#include "storage/table/in_mem_chunked_node_group_collection.h"

namespace kuzu {
namespace storage {
class NodeTable;
class RelTable;
} // namespace storage
namespace main {
class ClientContext;
}
namespace processor {

struct KUZU_API BasePartitionerSharedState {
    storage::NodeTable* srcNodeTable;
    storage::NodeTable* dstNodeTable;
    storage::RelTable* relTable;

    explicit BasePartitionerSharedState()
        : srcNodeTable{nullptr}, dstNodeTable{nullptr}, relTable(nullptr) {}
    virtual ~BasePartitionerSharedState() = default;

    virtual void initialize(const common::logical_type_vec_t& columnTypes,
        common::idx_t numPartitioners, const main::ClientContext* clientContext) = 0;

    virtual common::partition_idx_t getNextPartition(common::idx_t partitioningIdx) = 0;
    virtual common::partition_idx_t getNumPartitions(common::idx_t partitioningIdx) const = 0;
    virtual common::offset_t getNumNodes(common::idx_t partitioningIdx) const = 0;

    virtual void resetState() = 0;
    virtual void resetBuffers(common::idx_t partitioningIdx) = 0;

    // Must only be called once for any given parameters.
    // The data gets moved out of the shared state since some of it may be spilled to disk and will
    // need to be freed after its processed.
    virtual std::unique_ptr<storage::InMemChunkedNodeGroupCollection> getPartitionBuffer(
        common::idx_t partitioningIdx, common::partition_idx_t partitionIdx) const = 0;

    static common::partition_idx_t getNumPartitionsFromRows(common::offset_t numRows);
};
} // namespace processor
} // namespace kuzu
