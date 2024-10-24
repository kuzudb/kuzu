#pragma once

#include "common/uniq_lock.h"
#include "storage/enums/residency_state.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class MemoryManager;

struct TableAddColumnState;
class NodeGroup;
struct NodeGroupScanState {
    // Index of committed but not yet checkpointed chunked group to scan.
    common::idx_t chunkedGroupIdx = 0;
    common::row_idx_t numScannedRows = 0;
    // State of each chunk in the checkpointed chunked group.
    std::vector<ChunkState> chunkStates;

    explicit NodeGroupScanState(common::idx_t numChunks) { chunkStates.resize(numChunks); }
    virtual ~NodeGroupScanState() = default;
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);

    virtual void resetState() {
        chunkedGroupIdx = 0;
        numScannedRows = 0;
        for (auto& chunkState : chunkStates) {
            chunkState.resetState();
        }
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& constCast() {
        return common::ku_dynamic_cast<const TARGETT&>(*this);
    }
};

class MemoryManager;
struct NodeGroupCheckpointState {
    std::vector<common::column_id_t> columnIDs;
    std::vector<std::unique_ptr<Column>> columns;
    FileHandle& dataFH;
    MemoryManager* mm;

    NodeGroupCheckpointState(std::vector<common::column_id_t> columnIDs,
        std::vector<std::unique_ptr<Column>> columns, FileHandle& dataFH, MemoryManager* mm)
        : columnIDs{std::move(columnIDs)}, columns{std::move(columns)}, dataFH{dataFH}, mm{mm} {}
    virtual ~NodeGroupCheckpointState() = default;

    template<typename T>
    const T& cast() const {
        return common::ku_dynamic_cast<const T&>(*this);
    }
    template<typename T>
    T& cast() {
        return common::ku_dynamic_cast<T&>(*this);
    }
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

static auto NODE_GROUP_SCAN_EMMPTY_RESULT = NodeGroupScanResult{};

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
          numRows{chunkedNodeGroup->getNumRows()}, nextRowToAppend{numRows}, capacity{capacity} {
        for (auto i = 0u; i < chunkedNodeGroup->getNumColumns(); i++) {
            dataTypes.push_back(chunkedNodeGroup->getColumnChunk(i).getDataType().copy());
        }
        const auto lock = chunkedGroups.lock();
        chunkedGroups.appendGroup(lock, std::move(chunkedNodeGroup));
    }
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        common::row_idx_t capacity, NodeGroupDataFormat format)
        : nodeGroupIdx{nodeGroupIdx}, format{format}, enableCompression{enableCompression},
          numRows{0}, nextRowToAppend{0}, capacity{capacity} {}
    virtual ~NodeGroup() = default;

    virtual bool isEmpty() const { return numRows.load() == 0; }
    virtual common::row_idx_t getNumRows() const { return numRows.load(); }
    void moveNextRowToAppend(common::row_idx_t numRowsToAppend) {
        nextRowToAppend += numRowsToAppend;
    }
    common::row_idx_t getNumRowsLeftToAppend() const { return capacity - nextRowToAppend; }
    bool isFull() const { return numRows.load() == capacity; }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    NodeGroupDataFormat getFormat() const { return format; }
    common::row_idx_t append(const transaction::Transaction* transaction,
        ChunkedNodeGroup& chunkedGroup, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);
    common::row_idx_t append(const transaction::Transaction* transaction,
        const std::vector<ColumnChunk*>& chunkedGroup, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);
    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);

    void merge(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    virtual void initializeScanState(transaction::Transaction* transaction,
        TableScanState& state) const;
    void initializeScanState(transaction::Transaction* transaction, const common::UniqLock& lock,
        TableScanState& state) const;
    virtual NodeGroupScanResult scan(transaction::Transaction* transaction,
        TableScanState& state) const;

    bool lookup(const common::UniqLock& lock, transaction::Transaction* transaction,
        const TableScanState& state);
    bool lookup(transaction::Transaction* transaction, const TableScanState& state);

    void update(transaction::Transaction* transaction, common::row_idx_t rowIdxInGroup,
        common::column_id_t columnID, const common::ValueVector& propertyVector);
    bool delete_(const transaction::Transaction* transaction, common::row_idx_t rowIdxInGroup);

    bool hasDeletions(const transaction::Transaction* transaction);
    virtual void addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState, FileHandle* dataFH);

    void flush(transaction::Transaction* transaction, FileHandle& dataFH);

    virtual void checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state);

    bool hasChanges();
    uint64_t getEstimatedMemoryUsage();

    virtual void serialize(common::Serializer& serializer);
    static std::unique_ptr<NodeGroup> deserialize(MemoryManager& memoryManager,
        common::Deserializer& deSer);

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
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& cast() const {
        return common::ku_dynamic_cast<const TARGETT&>(*this);
    }

    bool isVisible(const transaction::Transaction* transaction, common::row_idx_t rowIdxInGroup);
    bool isVisibleNoLock(const transaction::Transaction* transaction,
        common::row_idx_t rowIdxInGroup);
    bool isDeleted(const transaction::Transaction* transaction, common::offset_t offsetInGroup);
    bool isInserted(const transaction::Transaction* transaction, common::offset_t offsetInGroup);

private:
    ChunkedNodeGroup* findChunkedGroupFromRowIdx(const common::UniqLock& lock,
        common::row_idx_t rowIdx);
    ChunkedNodeGroup* findChunkedGroupFromRowIdxNoLock(common::row_idx_t rowIdx);

    std::unique_ptr<ChunkedNodeGroup> checkpointInMemOnly(MemoryManager& memoryManager,
        const common::UniqLock& lock, NodeGroupCheckpointState& state);
    std::unique_ptr<ChunkedNodeGroup> checkpointInMemAndOnDisk(MemoryManager& memoryManager,
        const common::UniqLock& lock, NodeGroupCheckpointState& state);
    std::unique_ptr<VersionInfo> checkpointVersionInfo(const common::UniqLock& lock,
        const transaction::Transaction* transaction);

    template<ResidencyState SCAN_RESIDENCY_STATE>
    common::row_idx_t getNumResidentRows(const common::UniqLock& lock) const;
    template<ResidencyState SCAN_RESIDENCY_STATE>
    std::unique_ptr<ChunkedNodeGroup> scanAllInsertedAndVersions(MemoryManager& memoryManager,
        const common::UniqLock& lock, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<const Column*>& columns) const;

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
    // TODO(Guodong): Remove this field.
    common::row_idx_t nextRowToAppend;
    common::row_idx_t capacity;
    std::vector<common::LogicalType> dataTypes;
    GroupCollection<ChunkedNodeGroup> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
