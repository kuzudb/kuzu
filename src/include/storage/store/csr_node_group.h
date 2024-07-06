#pragma once

#include <array>

#include "storage/storage_utils.h"
#include "storage/store/chunked_node_group_collection.h"
#include "storage/store/csr_chunked_node_group.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

using row_idx_vec_t = std::vector<common::row_idx_t>;

struct csr_list_t {
    common::row_idx_t startRow = common::INVALID_ROW_IDX;
    common::length_t length = 0;
};

enum class CSRNodeGroupScanSource : uint8_t {
    COMMITTED_PERSISTENT = 0,
    COMMITTED_IN_MEMORY = 1,
    UNCOMMITTED = 2,
    NONE = 10
};

// Store rows of a CSR list.
// If rows of the CSR list are stored in a sequential order, then `isSequential` is set to true.
// rowIndices consists of startRowIdx and length.
// Otherwise, rowIndices records the row indices of each row in the CSR list.
struct NodeCSRIndex {
    bool isSequential = false;
    row_idx_vec_t rowIndices;

    common::row_idx_t getNumRows() const {
        if (isSequential) {
            return rowIndices[1];
        }
        return rowIndices.size();
    }

    void clear() {
        isSequential = false;
        rowIndices.clear();
    }
};

struct CSRIndex {
    std::array<NodeCSRIndex, common::StorageConstants::NODE_GROUP_SIZE> indices;

    common::row_idx_t getNumRows(common::offset_t offset) const {
        return indices[offset].getNumRows();
    }
};

class CSRNodeGroup;
struct CSRNodeGroupScanState final : NodeGroupScanState {
    // States at the node group level. Cached during scan over all csr lists within the same node
    // group. State for reading from checkpointed node group.
    std::unique_ptr<ChunkedCSRHeader> csrHeader;
    csr_list_t persistentCSRList;
    NodeCSRIndex inMemCSRList;

    // States at the csr list level. Cached during scan over a single csr list.
    CSRNodeGroupScanSource source = CSRNodeGroupScanSource::COMMITTED_PERSISTENT;

    explicit CSRNodeGroupScanState(common::idx_t numChunks)
        : NodeGroupScanState{numChunks},
          csrHeader{std::make_unique<ChunkedCSRHeader>(false,
              common::StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY)} {}

    void resetState() override {
        NodeGroupScanState::resetState();
        csrHeader->resetToEmpty();
        source = CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
        persistentCSRList = csr_list_t();
    }
};

struct RelTableScanState;

static constexpr common::column_id_t NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t REL_ID_COLUMN_ID = 1;

// Data in a CSRNodeGroup is organized as follows:
// - persistent data: checkpointed data or flushed data from batch insert. `persistentChunkGroup`.
// - transient data: data that is being committed but kept in memory. `chunkedGroups`.
// Persistent data are organized in CSR format.
// Transient data are organized similar to normal node groups. Tuples are always appended to the end
// of `chunkedGroups`. We keep an extra csrIndex to track the vector of row indices for each bound
// node.
class CSRNodeGroup final : public NodeGroup {
public:
    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::vector<common::LogicalType> dataTypes)
        : NodeGroup{nodeGroupIdx, enableCompression, std::move(dataTypes), common::INVALID_OFFSET,
              NodeGroupDataFormat::CSR} {}
    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup)
        : NodeGroup{nodeGroupIdx, enableCompression, std::move(chunkedNodeGroup),
              common::INVALID_OFFSET, NodeGroupDataFormat::CSR} {}

    void initializeScanState(transaction::Transaction* transaction, TableScanState& state) override;
    NodeGroupScanResult scan(transaction::Transaction* transaction, TableScanState& state) override;

    void appendChunkedCSRGroup(const transaction::Transaction* transaction,
        ChunkedCSRNodeGroup& chunkedGroup);
    void append(const transaction::Transaction* transaction, common::offset_t boundOffsetInGroup,
        const std::vector<ColumnChunk*>& chunks, common::row_idx_t startRowInChunks,
        common::row_idx_t numRows);

    void update(transaction::Transaction* transaction, CSRNodeGroupScanSource source,
        common::row_idx_t rowIdxInGroup, common::column_id_t columnID,
        const common::ValueVector& propertyVector);
    bool delete_(const transaction::Transaction* transaction, CSRNodeGroupScanSource source,
        common::row_idx_t rowIdxInGroup);

    void addColumn(transaction::Transaction* transaction, TableAddColumnState& addColumnState,
        BMFileHandle* dataFH) override;

    bool isEmpty() const override { return !persistentChunkGroup && NodeGroup::isEmpty(); }

    void setPersistentChunkedGroup(std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup) {
        KU_ASSERT(chunkedNodeGroup->getFormat() == NodeGroupDataFormat::CSR);
        persistentChunkGroup = std::move(chunkedNodeGroup);
    }

private:
    void initializePersistentCSRHeader(transaction::Transaction* transaction,
        RelTableScanState& relScanState, CSRNodeGroupScanState& nodeGroupScanState) const;

    void updateCSRIndex(common::offset_t boundNodeOffsetInGroup, common::row_idx_t startRow,
        common::length_t length) const;

    NodeGroupScanResult scanCommittedPersistent(const transaction::Transaction* transaction,
        const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const;
    NodeGroupScanResult scanCommittedInMemSequential(const transaction::Transaction* transaction,
        const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState);
    NodeGroupScanResult scanCommittedInMemRandom(transaction::Transaction* transaction,
        const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState);

private:
    std::unique_ptr<ChunkedNodeGroup> persistentChunkGroup;
    std::unique_ptr<CSRIndex> csrIndex;
};

} // namespace storage
} // namespace kuzu
