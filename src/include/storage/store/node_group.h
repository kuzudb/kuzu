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

    explicit NodeGroupScanState(common::idx_t numChunks) { chunkStates.resize(numChunks); }
    virtual ~NodeGroupScanState() = default;
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);

    virtual void resetState() {
        chunkedGroupIdx = 0;
        nextRowToScan = 0;
        for (auto& chunkState : chunkStates) {
            chunkState.resetState();
        }
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<NodeGroupScanState&, TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& constCast() {
        return common::ku_dynamic_cast<const NodeGroupScanState&, const TARGETT&>(*this);
    }
};

struct NodeGroupCheckpointState {
    std::vector<common::column_id_t> columnIDs;
    std::vector<Column*> columns;
    BMFileHandle& dataFH;
};

struct NodeGroupScanResult {

    common::row_idx_t startRow = common::INVALID_ROW_IDX;
    common::row_idx_t numRows = 0;

    constexpr NodeGroupScanResult() noexcept = default;
    constexpr NodeGroupScanResult(common::row_idx_t startRow, common::row_idx_t numRows) noexcept
        : startRow{startRow}, numRows{numRows} {}

    bool operator==(const NodeGroupScanResult& other) const {
        return startRow == other.startRow && numRows == other.numRows;
    }
};

static NodeGroupScanResult NODE_GROUP_SCAN_EMMPTY_RESULT = NodeGroupScanResult{};

struct TableScanState;
class NodeGroup {
public:
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::vector<common::LogicalType> dataTypes,
        common::row_idx_t capacity = common::StorageConstants::NODE_GROUP_SIZE,
        NodeGroupDataFormat format = NodeGroupDataFormat::REGULAR)
        : nodeGroupIdx{nodeGroupIdx}, format{format}, enableCompression{enableCompression},
          numRows{0}, nextRowToAppend{0}, capacity{capacity}, dataTypes{std::move(dataTypes)} {}
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup,
        common::row_idx_t capacity = common::StorageConstants::NODE_GROUP_SIZE,
        NodeGroupDataFormat format = NodeGroupDataFormat::REGULAR)
        : nodeGroupIdx{nodeGroupIdx}, format{format}, enableCompression{enableCompression},
          numRows{0}, nextRowToAppend{0}, capacity{capacity} {
        for (auto i = 0u; i < chunkedNodeGroup->getNumColumns(); i++) {
            dataTypes.push_back(chunkedNodeGroup->getColumnChunk(i).getDataType().copy());
        }
        const auto lock = chunkedGroups.lock();
        chunkedGroups.appendGroup(lock, std::move(chunkedNodeGroup));
    }
    virtual ~NodeGroup() = default;

    virtual bool isEmpty() const { return numRows.load() == 0; }
    common::row_idx_t getNumRows() const { return numRows.load(); }
    void moveNextRowToAppend(common::row_idx_t numRowsToAppend) {
        nextRowToAppend += numRowsToAppend;
    }
    common::row_idx_t getNumRowsLeftToAppend() const { return capacity - nextRowToAppend; }
    bool isFull() const { return numRows.load() == capacity; }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    common::row_idx_t append(const transaction::Transaction* transaction,
        ChunkedNodeGroup& chunkedGroup, common::row_idx_t numRowsToAppend);
    common::row_idx_t append(const transaction::Transaction* transaction,
        const std::vector<ColumnChunk*>& chunkedGroup, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);
    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);

    void merge(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    virtual void initializeScanState(transaction::Transaction* transaction, TableScanState& state);
    virtual NodeGroupScanResult scan(transaction::Transaction* transaction, TableScanState& state);
    bool lookup(transaction::Transaction* transaction, const TableScanState& state);

    void update(transaction::Transaction* transaction, common::row_idx_t rowIdxInGroup,
        common::column_id_t columnID, const common::ValueVector& propertyVector);
    bool delete_(const transaction::Transaction* transaction, common::row_idx_t rowIdxInGroup);

    void flush(BMFileHandle& dataFH);

    void checkpoint(const NodeGroupCheckpointState& state);

    bool hasChanges();
    uint64_t getEstimatedMemoryUsage();

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<NodeGroup> deserialize(common::Deserializer& deSer);

    common::node_group_idx_t getNumChunkedGroups() {
        const auto lock = chunkedGroups.lock();
        return chunkedGroups.getNumGroups(lock);
    }
    ChunkedNodeGroup* getChunkedNodeGroup(common::node_group_idx_t groupIdx) {
        const auto lock = chunkedGroups.lock();
        return chunkedGroups.getGroup(lock, groupIdx);
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<NodeGroup&, TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& cast() const {
        return common::ku_dynamic_cast<const NodeGroup&, const TARGETT&>(*this);
    }

private:
    ChunkedNodeGroup* findChunkedGroupFromRowIdx(const common::UniqLock& lock,
        common::row_idx_t rowIdx);

    template<ResidencyState SCAN_RESIDENCY_STATE>
    std::unique_ptr<ChunkedNodeGroup> scanCommitted(
        const std::vector<common::column_id_t>& columnIDs, const std::vector<Column*>& columns);

    static void populateNodeID(common::ValueVector& nodeIDVector, common::table_id_t tableID,
        common::offset_t startNodeOffset, common::row_idx_t numRows);

protected:
    common::node_group_idx_t nodeGroupIdx;
    NodeGroupDataFormat format;
    bool enableCompression;
    std::atomic<common::row_idx_t> numRows;
    // `nextRowToAppend` is a cursor to allow us to pre-reserve a set of rows to append before
    // acutally appending data. This is an optimization to reduce lock-contention when appending in
    // parallel.
    common::row_idx_t nextRowToAppend;
    common::row_idx_t capacity;
    std::vector<common::LogicalType> dataTypes;
    GroupCollection<ChunkedNodeGroup> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
