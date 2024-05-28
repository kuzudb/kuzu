#pragma once

#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct NodeGroupScanState {

    common::vector_idx_t chunkedGroupIdx = 0;
    common::row_idx_t nextRowToScan = 0;
    common::row_idx_t maxNumRowsToScan = 0;

    NodeGroupScanState() {}
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);

    bool hasNext() const { return nextRowToScan < maxNumRowsToScan; }
};

enum class NodeGroupType : uint8_t { IN_MEMORY, ON_DISK };

struct TableScanState;
class NodeGroup {
public:
    explicit NodeGroup(common::node_group_idx_t nodeGroupIdx,
        const std::vector<common::LogicalType>& dataTypes)
        : nodeGroupIdx{nodeGroupIdx}, dataTypes{dataTypes}, type{NodeGroupType::IN_MEMORY},
          startNodeOffset{0}, chunkedGroups{dataTypes} {}

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }
    bool isFull() const {
        return chunkedGroups.getNumRows() == common::StorageConstants::NODE_GROUP_SIZE;
    }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    void append(const ChunkedNodeGroupCollection& chunkCollection, common::row_idx_t offset,
        common::row_idx_t numRowsToAppend) {
        chunkedGroups.append(chunkCollection, offset, numRowsToAppend);
    }
    void append(transaction::Transaction* transaction, const ChunkedNodeGroup& chunkedGroup);
    void merge(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    void initializeScanState(transaction::Transaction* transaction, TableScanState& state) const;
    void scan(transaction::Transaction* transaction, TableScanState& state) const;

    void flush(BMFileHandle& dataFH);

    common::vector_idx_t getNumChunkedGroups() const { return chunkedGroups.getNumChunkedGroups(); }
    const ChunkedNodeGroup& getChunkedGroup(common::vector_idx_t idx) const {
        return chunkedGroups.getChunkedGroup(idx);
    }

    NodeGroupType getType() const { return type; }

private:
    void setToOnDisk() { type = NodeGroupType::ON_DISK; }

private:
    common::node_group_idx_t nodeGroupIdx;
    std::vector<common::LogicalType> dataTypes;
    NodeGroupType type;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    ChunkedNodeGroupCollection chunkedGroups;
};

} // namespace storage
} // namespace kuzu
