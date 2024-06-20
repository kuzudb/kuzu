#pragma once

#include <map>

#include "storage/enums/residency_state.h"
#include "storage/storage_utils.h"
#include "storage/store/chunked_node_group_collection.h"
#include "storage/store/version_info.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class NodeGroup;
struct NodeGroupScanState {
    // Index of committed but not yet checkpointed chunked group to scan.
    common::idx_t chunkedGroupIdx = 0;
    common::row_idx_t nextRowToScan = 0;
    // State of each chunk in the checkpointed chunked group.
    std::vector<ChunkState> chunkStates;

    NodeGroupScanState() {}
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);

    void resetState() {
        chunkedGroupIdx = 0;
        nextRowToScan = 0;
        for (auto& chunkState : chunkStates) {
            chunkState.resetState();
        }
    }
};

struct NodeGroupCheckpointState {
    std::vector<common::column_id_t> columnIDs;
    std::vector<Column*> columns;
    BMFileHandle& dataFH;
};

struct TableScanState;
class NodeGroup {
public:
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        const std::vector<common::LogicalType>& dataTypes,
        common::row_idx_t capacity = common::StorageConstants::NODE_GROUP_SIZE)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression}, numRows{0},
          nextRowToAppend{0}, capacity{capacity}, dataTypes{dataTypes},
          startNodeOffset{StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx)} {}
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup,
        common::row_idx_t capacity = common::StorageConstants::NODE_GROUP_SIZE)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression}, numRows{0},
          nextRowToAppend{0}, capacity{capacity},
          startNodeOffset{StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx)} {
        for (auto i = 0u; i < chunkedNodeGroup->getNumColumns(); i++) {
            dataTypes.push_back(chunkedNodeGroup->getColumnChunk(i).getDataType());
        }
        const auto lock = chunkedGroups.lock();
        chunkedGroups.appendGroup(lock, std::move(chunkedNodeGroup));
    }

    common::row_idx_t getNumRows() const { return numRows.load(); }
    void moveNextRowToAppend(common::row_idx_t numRowsToAppend) {
        nextRowToAppend += numRowsToAppend;
    }
    common::row_idx_t getNumRowsLeftToAppend() const { return capacity - nextRowToAppend; }
    bool isFull() const { return numRows.load() == capacity; }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    common::row_idx_t append(const transaction::Transaction* transaction,
        const ChunkedNodeGroup& chunkedGroup, common::row_idx_t numRowsToAppend);
    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);

    void merge(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    void initializeScanState(transaction::Transaction* transaction, const TableScanState& state,
        NodeGroupScanState& nodeGroupScanState);
    bool scan(transaction::Transaction* transaction, TableScanState& state,
        NodeGroupScanState& nodeGroupScanState);
    void lookup(transaction::Transaction* transaction, TableScanState& state,
        NodeGroupScanState& nodeGroupScanState);

    void update(transaction::Transaction* transaction, common::offset_t offset,
        common::column_id_t columnID, common::ValueVector& propertyVector);
    bool delete_(transaction::Transaction* transaction, common::offset_t offset);

    void flush(BMFileHandle& dataFH);

    void checkpoint(const NodeGroupCheckpointState& state);

    bool hasChanges();
    uint64_t getEstimatedMemoryUsage();

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<NodeGroup> deserialize(common::Deserializer& deSer);

    common::offset_t getStartNodeOffset() const { return startNodeOffset; }

    common::node_group_idx_t getNumChunkedGroups() {
        const auto lock = chunkedGroups.lock();
        return chunkedGroups.getNumGroups(lock);
    }

private:
    template<ResidencyState SCAN_RESIDENCY_STATE>
    std::unique_ptr<ChunkedNodeGroup> scanCommitted(
        const std::vector<common::column_id_t>& columnIDs, const std::vector<Column*>& columns);

    static void populateNodeID(common::ValueVector& nodeIDVector, common::table_id_t tableID,
        common::offset_t startNodeOffset, common::row_idx_t numRows);

private:
    common::node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    std::atomic<common::row_idx_t> numRows;
    // `nextRowToAppend` is a cursor to allow us to pre-reserve a set of rows to append before
    // acutally appending data. This is an optimization to reduce lock-contention when appending in
    // parallel.
    common::row_idx_t nextRowToAppend;
    common::row_idx_t capacity;
    std::vector<common::LogicalType> dataTypes;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    // std::unique_ptr<ChunkedNodeGroup> checkpointedGroup;
    GroupCollection<ChunkedNodeGroup> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
