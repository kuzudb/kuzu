#pragma once

#include "storage/store/csr_node_group.h"
#include "storage/store/group_collection.h"

namespace kuzu {
namespace storage {
// struct NodeGroupCheckpointState;

// class CSRNodeGroupCollection {
// public:
//     CSRNodeGroupCollection(const std::vector<common::LogicalType>& types, bool enableCompression,
//         common::offset_t startNodeOffset = 0, BMFileHandle* dataFH = nullptr,
//         common::Deserializer* deSer = nullptr);
//
//     void append(transaction::Transaction* transaction, common::ValueVector* srcVector,
//         std::vector<common::ValueVector*> dataVectors);
//
//     CSRNodeGroup& getNodeGroup(const common::node_group_idx_t nodeGroupIdx) {
//         const auto lock = nodeGroups.lock();
//         const auto group = nodeGroups.getGroup(lock, nodeGroupIdx);
//         KU_ASSERT(group);
//         return *group;
//     }
//     void setNodeGroup(const common::node_group_idx_t nodeGroupIdx,
//         std::unique_ptr<CSRNodeGroup> group) {
//         const auto lock = nodeGroups.lock();
//         nodeGroups.replaceGroup(lock, nodeGroupIdx, std::move(group));
//     }
//
//     void clear() {
//         const auto lock = nodeGroups.lock();
//         nodeGroups.clear(lock);
//     }
//
//     common::row_idx_t getNumRows() const { return numRows.load(); }
//
//     uint64_t getEstimatedMemoryUsage();
//
//     void checkpoint(const NodeGroupCheckpointState& state);
//
//     void serialize(common::Serializer& ser) { nodeGroups.serializeGroups(ser); }
//
// private:
//     bool enableCompression;
//     common::offset_t startNodeOffset;
//     std::atomic<common::row_idx_t> numRows;
//     std::vector<common::LogicalType> types;
//     BMFileHandle* dataFH;
//     GroupCollection<CSRNodeGroup> nodeGroups;
// };

} // namespace storage
} // namespace kuzu
