#pragma once

#include <mutex>

#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class NodeGroupCollection {
public:
    struct AppendState {
        std::vector<NodeGroupAppendState> nodeGroupAppendState;
        common::row_idx_t startRowIdx = common::INVALID_ROW_IDX;
        common::row_idx_t currentRowToAppend = 0;
        common::row_idx_t numRowsToAppend = common::INVALID_ROW_IDX;

        explicit AppendState(common::row_idx_t numRowsToAppend)
            : numRowsToAppend{numRowsToAppend} {}
    };

    explicit NodeGroupCollection(const std::vector<common::LogicalType>& types,
        bool enableCompression, common::offset_t startNodeOffset = 0);
    NodeGroupCollection(const std::vector<common::LogicalType>& types, bool enableCompression,
        BMFileHandle* dataFH, common::Deserializer* deSer);

    // void initializeAppend(AppendState& appendState);
    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors);
    void append(const transaction::Transaction* transaction,
        const ChunkedNodeGroupCollection& chunkedGroupCollection);

    // Rename this to mergeCollection.
    // void append(const transaction::Transaction* transaction, const NodeGroupCollection& other);

    std::pair<common::offset_t, common::offset_t> appendPartially(
        transaction::Transaction* transaction, ChunkedNodeGroup& chunkedGroup);

    common::row_idx_t getNumRows() const;
    common::node_group_idx_t getNumNodeGroups() { return nodeGroups.size(); }
    NodeGroup& findNodeGroupFromOffset(common::offset_t offset);
    NodeGroup& getNodeGroup(const common::node_group_idx_t groupIdx) {
        KU_ASSERT(groupIdx < nodeGroups.size());
        return *nodeGroups[groupIdx];
    }
    void setNodeGroup(const common::node_group_idx_t nodeGroupIdx,
        std::unique_ptr<NodeGroup> group) {
        nodeGroups.resize(nodeGroupIdx + 1);
        nodeGroups[nodeGroupIdx] = std::move(group);
    }

    // void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
    // const ChunkedNodeGroup& chunkedGroup);

    uint64_t getEstimatedMemoryUsage() const;

    void checkpoint(const NodeGroupCheckpointState& state);

    void serialize(common::Serializer& ser) const;

private:
    std::mutex mtx;
    bool enableCompression;
    common::offset_t startNodeOffset;
    std::atomic<common::row_idx_t> numRows;
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<NodeGroup>> nodeGroups;
    BMFileHandle* dataFH;
};

} // namespace storage
} // namespace kuzu
