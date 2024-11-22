#pragma once

#include <array>
#include <bitset>

#include "common/data_chunk/data_chunk.h"
#include "storage/store/csr_chunked_node_group.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

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
    // TODO(Guodong): Should seperate `isSequential` and `rowIndices` to two different data
    // structures. so the struct can be more space efficient.
    bool isSequential = false;
    row_idx_vec_t rowIndices; // TODO(Guodong): Should optimze the vector to a more space-efficient
                              // data structure.

    bool isEmpty() const { return getNumRows() == 0; }
    common::row_idx_t getNumRows() const {
        if (isSequential) {
            return rowIndices[1];
        }
        return rowIndices.size();
    }
    row_idx_vec_t getRows() const {
        if (isSequential) {
            row_idx_vec_t result;
            result.reserve(rowIndices[1]);
            for (common::row_idx_t i = 0u; i < rowIndices[1]; ++i) {
                result.push_back(i + rowIndices[0]);
            }
            return result;
        }
        return rowIndices;
    }

    void clear() {
        isSequential = false;
        rowIndices.clear();
    }

    void turnToNonSequential() {
        if (isSequential) {
            row_idx_vec_t newIndices;
            newIndices.reserve(rowIndices[1]);
            for (common::row_idx_t i = 0u; i < rowIndices[1]; ++i) {
                newIndices.push_back(i + rowIndices[0]);
            }
            rowIndices = std::move(newIndices);
            isSequential = false;
        }
    }
    void setInvalid(common::idx_t idx) {
        KU_ASSERT(!isSequential);
        KU_ASSERT(idx < rowIndices.size());
        rowIndices[idx] = common::INVALID_ROW_IDX;
    }
};

// TODO(Guodong): Split CSRIndex into two levels: one level per csr leaf region, another per node
// under the leaf region. This should be more space efficient.
struct CSRIndex {
    std::array<NodeCSRIndex, common::StorageConstants::NODE_GROUP_SIZE> indices;

    common::row_idx_t getNumRows(common::offset_t offset) const {
        return indices[offset].getNumRows();
    }

    common::offset_t getMaxOffsetWithRels() const {
        common::offset_t maxOffset = 0;
        for (auto i = 0u; i < indices.size(); i++) {
            if (!indices[i].isEmpty()) {
                maxOffset = i;
            }
        }
        return maxOffset;
    }
};

// TODO(Guodong): Serialize the info to disk. This should be a config per node group.
struct PackedCSRInfo {
    uint64_t calibratorTreeHeight = common::StorageConstants::NODE_GROUP_SIZE_LOG2 -
                                    common::StorageConstants::CSR_LEAF_REGION_SIZE_LOG2;
    double highDensityStep = (common::StorageConstants::LEAF_HIGH_CSR_DENSITY -
                                 common::StorageConstants::PACKED_CSR_DENSITY) /
                             static_cast<double>(calibratorTreeHeight);

    constexpr PackedCSRInfo() noexcept = default;
};

class CSRNodeGroup;
struct RelTableScanState;
struct CSRNodeGroupScanState final : NodeGroupScanState {
    // Cached offsets and lengths for a sequence of CSR lists within the current vector of
    // boundNodes.
    std::unique_ptr<ChunkedCSRHeader> header;

    std::optional<std::bitset<common::DEFAULT_VECTOR_CAPACITY>> cachedScannedVectorsSelBitset;
    // The total number of rows (i.e., rels) in the current node group.
    common::row_idx_t numTotalRows;
    // The number of rows (i.e., rels) that have been scanned so far in current node group.
    common::row_idx_t numCachedRows;
    common::row_idx_t nextCachedRowToScan;

    // States at the csr list level. Cached during scan over a single csr list.
    NodeCSRIndex inMemCSRList;

    CSRNodeGroupScanSource source;

    explicit CSRNodeGroupScanState(common::idx_t numChunks)
        : NodeGroupScanState{numChunks}, header{nullptr}, numTotalRows{0}, numCachedRows{0},
          nextCachedRowToScan{0}, source{CSRNodeGroupScanSource::COMMITTED_PERSISTENT} {}
    CSRNodeGroupScanState(MemoryManager& mm, common::idx_t numChunks)
        : NodeGroupScanState{numChunks}, numTotalRows{0}, numCachedRows{0}, nextCachedRowToScan{0},
          source{CSRNodeGroupScanSource::COMMITTED_PERSISTENT} {
        header = std::make_unique<ChunkedCSRHeader>(mm, false,
            common::StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    }

    bool tryScanCachedTuples(RelTableScanState& tableScanState);
};

struct CSRNodeGroupCheckpointState final : NodeGroupCheckpointState {
    Column* csrOffsetColumn;
    Column* csrLengthColumn;

    std::unique_ptr<ChunkedCSRHeader> oldHeader;
    std::unique_ptr<ChunkedCSRHeader> newHeader;

    CSRNodeGroupCheckpointState(std::vector<common::column_id_t> columnIDs,
        std::vector<Column*> columns, FileHandle& dataFH, MemoryManager* mm, Column* csrOffsetCol,
        Column* csrLengthCol)
        : NodeGroupCheckpointState{std::move(columnIDs), std::move(columns), dataFH, mm},
          csrOffsetColumn{csrOffsetCol}, csrLengthColumn{csrLengthCol} {}
};

static constexpr common::column_id_t NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t REL_ID_COLUMN_ID = 1;

// Data in a CSRNodeGroup is organized as follows:
// - persistent data: checkpointed data or flushed data from batch insert. `persistentChunkGroup`.
// - transient data: data that is being committed but kept in memory. `chunkedGroups`.
// Persistent data are organized in CSR format.
// Transient data are organized similar to normal node groups. Tuples are always appended to the end
// of `chunkedGroups`. We keep an extra csrIndex to track the vector of row indices for each bound
// node.
struct RelTableScanState;
class CSRNodeGroup final : public NodeGroup {
public:
    static constexpr PackedCSRInfo DEFAULT_PACKED_CSR_INFO{};

    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::vector<common::LogicalType> dataTypes)
        : NodeGroup{nodeGroupIdx, enableCompression, std::move(dataTypes), common::INVALID_OFFSET,
              NodeGroupDataFormat::CSR} {}
    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup)
        : NodeGroup{nodeGroupIdx, enableCompression, common::INVALID_OFFSET,
              NodeGroupDataFormat::CSR},
          persistentChunkGroup{std::move(chunkedNodeGroup)} {
        for (auto i = 0u; i < persistentChunkGroup->getNumColumns(); i++) {
            dataTypes.push_back(persistentChunkGroup->getColumnChunk(i).getDataType().copy());
        }
    }

    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& state) const override;
    NodeGroupScanResult scan(transaction::Transaction* transaction,
        TableScanState& state) const override;

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
        FileHandle* dataFH) override;

    void checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state) override;

    bool isEmpty() const override { return !persistentChunkGroup && NodeGroup::isEmpty(); }

    ChunkedNodeGroup* getPersistentChunkedGroup() const { return persistentChunkGroup.get(); }
    void setPersistentChunkedGroup(std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup) {
        KU_ASSERT(chunkedNodeGroup->getFormat() == NodeGroupDataFormat::CSR);
        persistentChunkGroup = std::move(chunkedNodeGroup);
    }

    void serialize(common::Serializer& serializer) override;

private:
    void initScanForCommittedPersistent(transaction::Transaction* transaction,
        RelTableScanState& relScanState, CSRNodeGroupScanState& nodeGroupScanState) const;
    void initScanForCommittedInMem(RelTableScanState& relScanState,
        CSRNodeGroupScanState& nodeGroupScanState) const;

    void updateCSRIndex(common::offset_t boundNodeOffsetInGroup, common::row_idx_t startRow,
        common::length_t length) const;

    NodeGroupScanResult scanCommittedPersistent(const transaction::Transaction* transaction,
        RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const;
    NodeGroupScanResult scanCommittedPersistentWithCache(
        const transaction::Transaction* transaction, RelTableScanState& tableState,
        CSRNodeGroupScanState& nodeGroupScanState) const;
    NodeGroupScanResult scanCommittedPersistentWtihoutCache(
        const transaction::Transaction* transaction, RelTableScanState& tableState,
        CSRNodeGroupScanState& nodeGroupScanState) const;

    NodeGroupScanResult scanCommittedInMem(transaction::Transaction* transaction,
        RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const;
    NodeGroupScanResult scanCommittedInMemSequential(const transaction::Transaction* transaction,
        const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const;
    NodeGroupScanResult scanCommittedInMemRandom(transaction::Transaction* transaction,
        const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const;

    void checkpointInMemOnly(const common::UniqLock& lock, NodeGroupCheckpointState& state);
    void checkpointInMemAndOnDisk(const common::UniqLock& lock, NodeGroupCheckpointState& state);

    void populateCSRLengthInMemOnly(const common::UniqLock& lock, common::offset_t numNodes,
        const CSRNodeGroupCheckpointState& csrState);

    void collectRegionChangesAndUpdateHeaderLength(const common::UniqLock& lock, CSRRegion& region,
        const CSRNodeGroupCheckpointState& csrState);
    void collectInMemRegionChangesAndUpdateHeaderLength(const common::UniqLock& lock,
        CSRRegion& region, const CSRNodeGroupCheckpointState& csrState);
    void collectOnDiskRegionChangesAndUpdateHeaderLength(const common::UniqLock& lock,
        CSRRegion& region, const CSRNodeGroupCheckpointState& csrState) const;

    std::vector<CSRRegion> collectLeafRegionsAndCSRLength(const common::UniqLock& lock,
        const CSRNodeGroupCheckpointState& csrState);
    void collectPersistentUpdatesInRegion(CSRRegion& region, common::offset_t leftCSROffset,
        common::offset_t rightCSROffset) const;

    common::row_idx_t getNumDeletionsForNodeInPersistentData(common::offset_t nodeOffset,
        const CSRNodeGroupCheckpointState& csrState) const;

    static void initScanStateFromScanChunk(const CSRNodeGroupCheckpointState& csrState,
        const common::DataChunk& dataChunk, TableScanState& scanState);
    static void redistributeCSRRegions(const CSRNodeGroupCheckpointState& csrState,
        const std::vector<CSRRegion>& leafRegions);
    static std::vector<CSRRegion> mergeRegionsToCheckpoint(
        const CSRNodeGroupCheckpointState& csrState, std::vector<CSRRegion>& leafRegions);
    static bool isWithinDensityBound(const ChunkedCSRHeader& header,
        const std::vector<CSRRegion>& leafRegions, const CSRRegion& region);

    void checkpointColumn(const common::UniqLock& lock, common::column_id_t columnID,
        const CSRNodeGroupCheckpointState& csrState, const std::vector<CSRRegion>& regions);
    ChunkCheckpointState checkpointColumnInRegion(const common::UniqLock& lock,
        common::column_id_t columnID, const CSRNodeGroupCheckpointState& csrState,
        const CSRRegion& region);
    void checkpointCSRHeaderColumns(const CSRNodeGroupCheckpointState& csrState) const;
    void finalizeCheckpoint(const common::UniqLock& lock);

private:
    std::unique_ptr<ChunkedNodeGroup> persistentChunkGroup;
    std::unique_ptr<CSRIndex> csrIndex;
};

} // namespace storage
} // namespace kuzu
