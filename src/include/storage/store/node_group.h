#pragma once

#include <map>

#include "storage/store/chunked_node_group_collection.h"
#include "storage/store/version_info.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct NodeGroupScanState {
    common::idx_t chunkedGroupIdx = 0;
    common::row_idx_t nextRowToScan = 0;

    NodeGroupScanState() {}
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);
};

class TableData;
struct TableScanState;
class NodeGroup {
public:
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        const ResidencyState residencyState, const std::vector<common::LogicalType>& dataTypes)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression}, dataTypes{dataTypes},
          residencyState{residencyState}, startNodeOffset{0},
          chunkedGroups{residencyState, dataTypes} {}
    NodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup,
        std::unique_ptr<NodeGroupVersionInfo> versionInfo)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression},
          residencyState{ResidencyState::ON_DISK}, startNodeOffset{0},
          chunkedGroups{std::move(chunkedNodeGroup)} {
        if (versionInfo) {
            this->versionInfo = std::move(*versionInfo);
        }
    }

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }
    bool isFull() const {
        return chunkedGroups.getNumRows() == common::StorageConstants::NODE_GROUP_SIZE;
    }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    void append(const transaction::Transaction* transaction,
        const ChunkedNodeGroupCollection& chunkCollection, common::row_idx_t offset,
        common::row_idx_t numRowsToAppend);
    void append(const transaction::Transaction* transaction, const ChunkedNodeGroup& chunkedGroup);
    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors, common::row_idx_t startRowIdx,
        common::row_idx_t numRowsToAppend);

    void merge(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    void initializeScanState(transaction::Transaction* transaction, TableScanState& state) const;
    bool scan(transaction::Transaction* transaction, TableScanState& state) const;
    void lookup(transaction::Transaction* transaction, TableScanState& state) const;

    void update(transaction::Transaction* transaction, common::offset_t offset,
        common::column_id_t columnID, common::ValueVector& propertyVector);
    bool delete_(transaction::Transaction* transaction, common::offset_t offset);

    void flush(BMFileHandle& dataFH);

    void checkpoint(TableData& tableData, BMFileHandle& dataFH);

    bool hasChanges() const;
    bool hasInsertionsOrDeletions() const;
    uint64_t getEstimatedMemoryUsage() const;

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<NodeGroup> deserialize(common::Deserializer& deSer);

    common::idx_t getNumChunkedGroups() const { return chunkedGroups.getNumChunkedGroups(); }
    const ChunkedNodeGroup& getChunkedGroup(const common::idx_t idx) const {
        return chunkedGroups.getChunkedGroup(idx);
    }
    const ChunkedNodeGroupCollection& getChunkedGroups() const { return chunkedGroups; }

    ResidencyState getResidencyState() const { return residencyState; }
    common::offset_t getStartNodeOffset() const { return startNodeOffset; }

private:
    void setToOnDisk() { residencyState = ResidencyState::ON_DISK; }

    std::unique_ptr<ChunkedNodeGroup> scanInMemCommitted();
    ColumnChunkData* fetchOnDiskUpdates(common::column_id_t columnID,
        std::map<common::offset_t, common::row_idx_t>& updateInfo);

    static void populateNodeID(common::ValueVector& nodeIDVector,
        common::SelectionVector& selVector, common::table_id_t tableID,
        common::offset_t startNodeOffset, common::row_idx_t numRows);

private:
    common::node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    std::vector<common::LogicalType> dataTypes;
    ResidencyState residencyState;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    ChunkedNodeGroupCollection chunkedGroups;
    NodeGroupVersionInfo versionInfo;
};

} // namespace storage
} // namespace kuzu
