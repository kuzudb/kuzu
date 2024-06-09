#pragma once

#include <shared_mutex>

#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class TableData;

class NodeGroupCollection {
public:
    explicit NodeGroupCollection(const std::vector<common::LogicalType>& types,
        bool enableCompression, common::offset_t startNodeOffset = 0);
    NodeGroupCollection(const std::vector<common::LogicalType>& types, BMFileHandle* dataFH,
        const TableData& tableData);

    void append(transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors);
    void append(transaction::Transaction* transaction,
        const ChunkedNodeGroupCollection& chunkedGroupCollection);
    void append(transaction::Transaction* transaction, const NodeGroupCollection& other);

    common::row_idx_t getNumRows();
    common::node_group_idx_t getNumNodeGroups() {
        std::shared_lock sLck{mtx};
        return nodeGroups.size();
    }
    NodeGroup& findNodeGroupFromOffset(common::offset_t offset);
    NodeGroup& getNodeGroup(const common::node_group_idx_t groupIdx) {
        std::shared_lock sLck{mtx};
        KU_ASSERT(groupIdx < nodeGroups.size());
        return *nodeGroups[groupIdx];
    }
    void setNodeGroup(const common::node_group_idx_t nodeGroupIdx,
        std::unique_ptr<NodeGroup> group) {
        std::unique_lock xLck{mtx};
        nodeGroups.resize(nodeGroupIdx + 1);
        nodeGroups[nodeGroupIdx] = std::move(group);
    }

    void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        const ChunkedNodeGroup& chunkedGroup);

    void checkpoint();

private:
    std::shared_mutex mtx;
    bool enableCompression;
    common::offset_t startNodeOffset;
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<NodeGroup>> nodeGroups;
    BMFileHandle* dataFH;
    const TableData* tableData;
};

} // namespace storage
} // namespace kuzu
