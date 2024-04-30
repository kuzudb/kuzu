#pragma once

#include "common/enums/rel_direction.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class LocalRelNG;
struct RelDataReadState : public TableDataReadState {
    common::RelDataDirection direction;
    common::offset_t startNodeOffset;
    common::offset_t numNodes;
    common::offset_t currentNodeOffset;
    common::offset_t posInCurrentCSR;
    std::vector<common::list_entry_t> csrListEntries;
    // Temp auxiliary data structure to scan the offset of each CSR node in the offset column chunk.
    ChunkedCSRHeader csrHeaderChunks = ChunkedCSRHeader(false /*enableCompression*/);
    std::vector<Column::ChunkState> columnStates;

    bool readFromPersistentStorage;
    // Following fields are used for local storage.
    bool readFromLocalStorage;
    LocalRelNG* localNodeGroup;

    explicit RelDataReadState();
    DELETE_COPY_DEFAULT_MOVE(RelDataReadState);

    inline bool isOutOfRange(common::offset_t nodeOffset) const {
        return nodeOffset < startNodeOffset ||
               nodeOffset >= (startNodeOffset + common::StorageConstants::NODE_GROUP_SIZE);
    }
    bool hasMoreToRead(transaction::Transaction* transaction);
    void populateCSRListEntries();
    std::pair<common::offset_t, common::offset_t> getStartAndEndOffset();

    inline bool hasMoreToReadInPersistentStorage() {
        return (currentNodeOffset - startNodeOffset) < numNodes &&
               posInCurrentCSR < csrListEntries[(currentNodeOffset - startNodeOffset)].size;
    }
    bool hasMoreToReadFromLocalStorage() const;
    bool trySwitchToLocalStorage();
};

struct CSRHeaderColumns {
    std::unique_ptr<Column> offset;
    std::unique_ptr<Column> length;

    inline void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ChunkedCSRHeader& chunks) const {
        offset->scan(transaction, nodeGroupIdx, chunks.offset.get());
        length->scan(transaction, nodeGroupIdx, chunks.length.get());
    }
    inline void append(const ChunkedCSRHeader& headerChunks,
        common::node_group_idx_t nodeGroupIdx) const {
        offset->append(headerChunks.offset.get(), nodeGroupIdx);
        length->append(headerChunks.length.get(), nodeGroupIdx);
    }

    common::offset_t getNumNodes(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const;
};

// TODO(Guodong): Serialize the info to disk. This should be a config per node group.
struct PackedCSRInfo {
    uint64_t calibratorTreeHeight;
    double lowDensityStep;
    double highDensityStep;

    PackedCSRInfo();
};

struct PackedCSRRegion {
    common::vector_idx_t regionIdx = common::INVALID_VECTOR_IDX;
    common::vector_idx_t level = common::INVALID_VECTOR_IDX;
    int64_t sizeChange = 0;
    // Left most and right most node offset of the region.
    common::offset_t leftBoundary = common::INVALID_OFFSET;
    common::offset_t rightBoundary = common::INVALID_OFFSET;

    PackedCSRRegion() {}
    PackedCSRRegion(common::vector_idx_t regionIdx, common::vector_idx_t level);

    inline std::pair<common::offset_t, common::offset_t> getNodeOffsetBoundaries() const {
        return std::make_pair(leftBoundary, rightBoundary);
    }
    inline std::pair<common::vector_idx_t, common::vector_idx_t> getSegmentBoundaries() const {
        auto left = regionIdx << level;
        return std::make_pair(left, left + (1 << level) - 1);
    }
    inline bool isOutOfBoundary(common::offset_t nodeOffset) const {
        return nodeOffset < leftBoundary || nodeOffset > rightBoundary;
    }

    // Check if the other region's segments are all within the range of this region.
    bool isWithin(const PackedCSRRegion& other) const;

    void setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment);
};

class RelsStoreStats;
class RelTableData final : public TableData {
public:
    struct PersistentState {
        ChunkedCSRHeader header;
        std::unique_ptr<ColumnChunk> relIDChunk;
        common::offset_t leftCSROffset = common::INVALID_OFFSET;
        common::offset_t rightCSROffset = common::INVALID_OFFSET;

        explicit PersistentState(common::offset_t numNodes) {
            header = ChunkedCSRHeader(false /*enableCompression*/, numNodes);
        }
    };

    struct LocalState {
    public:
        LocalRelNG* localNG;
        ChunkedCSRHeader header;
        PackedCSRRegion region;
        std::vector<int64_t> sizeChangesPerSegment;
        std::vector<bool> hasChangesPerSegment;
        // Total num of rels in the node group after applying changes.
        common::offset_t regionSize = 0;
        // Total capacity of the node group after resizing if necessary.
        common::offset_t regionCapacity = 0;
        common::offset_t leftCSROffset = common::INVALID_OFFSET;
        common::offset_t rightCSROffset = common::INVALID_OFFSET;
        bool needSliding = false;

        explicit LocalState(LocalRelNG* localNG);

        inline void setRegion(PackedCSRRegion& region_) { region = region_; }
    };

    RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::TableCatalogEntry* tableEntry, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, const common::ValueVector& inNodeIDVector,
        RelDataReadState& readState);
    void scan(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;
    void lookup(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // TODO: Should be removed. This is used by detachDelete for now.
    bool delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* relIDVector);

    void checkRelMultiplicityConstraint(transaction::Transaction* transaction,
        common::ValueVector* srcNodeIDVector) const;
    bool checkIfNodeHasRels(transaction::Transaction* transaction,
        common::offset_t nodeOffset) const;
    void append(ChunkedNodeGroup* nodeGroup) override;

    inline Column* getNbrIDColumn() const { return columns[NBR_ID_COLUMN_ID].get(); }
    inline Column* getCSROffsetColumn() const { return csrHeaderColumns.offset.get(); }
    inline Column* getCSRLengthColumn() const { return csrHeaderColumns.length.get(); }

    bool isNewNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const;

    void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) override;

    void prepareCommitNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalRelNG* localRelNG);

    void prepareCommit() override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    inline common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const override {
        return columns[NBR_ID_COLUMN_ID]->getNumNodeGroups(transaction);
    }

private:
    static common::offset_t getMaxNumNodesInRegion(const ChunkedCSRHeader& header,
        const PackedCSRRegion& region, const LocalRelNG* localNG);

    void initializeColumnReadStates(transaction::Transaction* transaction,
        RelDataReadState& readState, common::node_group_idx_t nodeGroupIdx);

    std::vector<PackedCSRRegion> findRegions(const ChunkedCSRHeader& headerChunks,
        LocalState& localState);
    common::length_t getNewRegionSize(const ChunkedCSRHeader& header,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region);
    bool isWithinDensityBound(const ChunkedCSRHeader& headerChunks,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region);
    double getHighDensity(uint64_t level) const;

    void updateCSRHeader(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        PersistentState& persistentState, LocalState& localState);
    void commitCSRHeaderChunk(transaction::Transaction* transaction, bool isNewNodeGroup,
        common::node_group_idx_t nodeGroupIdx, Column* column, ColumnChunk* columnChunk,
        LocalState& localState, const std::vector<common::offset_t>& dstOffsets);

    void distributeOffsets(const ChunkedCSRHeader& header, LocalState& localState,
        common::offset_t leftBoundary, common::offset_t rightBoundary);
    void updateRegion(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, PersistentState& persistentState, LocalState& localState);
    void updateColumn(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, common::column_id_t columnID,
        const RelTableData::PersistentState& persistentState, LocalState& localState);
    void distributeAndUpdateColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        const PersistentState& persistentState, LocalState& localState);

    void findPositionsForInsertions(common::offset_t nodeOffset, common::length_t numInsertions,
        LocalState& localState);
    void slideForInsertions(common::offset_t nodeOffset, common::length_t numInsertions,
        LocalState& localState);
    void slideLeftForInsertions(common::offset_t nodeOffset, common::offset_t leftBoundary,
        LocalState& localState, uint64_t numValuesToInsert);
    void slideRightForInsertions(common::offset_t nodeOffset, common::offset_t rightBoundary,
        LocalState& localState, uint64_t numValuesToInsert);

    void applyUpdatesToChunk(const PersistentState& persistentState, LocalState& localState,
        const ChunkCollection& localChunk, ColumnChunk* chunk, common::column_id_t columnID);
    void applyInsertionsToChunk(const PersistentState& persistentState,
        const LocalState& localState, const ChunkCollection& localChunk, ColumnChunk* chunk);
    void applyDeletionsToChunk(const PersistentState& persistentState, const LocalState& localState,
        ColumnChunk* chunk);

    void applyUpdatesToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        const PersistentState& persistentState, LocalState& localState, Column* column);
    void applyInsertionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        LocalState& localState, const PersistentState& persistentState, Column* column);
    void applyDeletionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, LocalState& localState,
        const PersistentState& persistentState, Column* column);
    void applySliding(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, LocalState& localState, const PersistentState& persistentState,
        Column* column);

    std::vector<std::pair<common::offset_t, common::offset_t>> getSlidesForDeletions(
        const PersistentState& persistentState, const LocalState& localState);

    LocalRelNG* getLocalNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx);

    template<typename T1, typename T2>
    static double divideNoRoundUp(T1 v1, T2 v2) {
        static_assert(std::is_arithmetic<T1>::value && std::is_arithmetic<T2>::value);
        return (double)v1 / (double)v2;
    }
    template<typename T1, typename T2>
    static uint64_t multiplyAndRoundUpTo(T1 v1, T2 v2) {
        static_assert(std::is_arithmetic<T1>::value && std::is_arithmetic<T2>::value);
        return std::ceil((double)v1 * (double)v2);
    }

    inline void fillSequence(std::span<common::offset_t> offsets, common::offset_t startOffset) {
        for (auto i = 0u; i < offsets.size(); i++) {
            offsets[i] = i + startOffset;
        }
    }

    common::offset_t findCSROffsetInRegion(const PersistentState& persistentState,
        common::offset_t nodeOffset, common::offset_t relOffset) const;

private:
    PackedCSRInfo packedCSRInfo;
    CSRHeaderColumns csrHeaderColumns;
    common::RelDataDirection direction;
    common::RelMultiplicity multiplicity;
};

} // namespace storage
} // namespace kuzu
