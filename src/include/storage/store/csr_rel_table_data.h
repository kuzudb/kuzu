#pragma once

#include "common/types/types.h"
#include "storage/store/rel_table_data.h"

namespace kuzu {
namespace storage {

using density_range_t = std::pair<double, double>;

struct CSRHeaderColumns {
    std::unique_ptr<Column> offset;
    std::unique_ptr<Column> length;

    inline void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        CSRHeaderChunks& chunks) const {
        offset->scan(transaction, nodeGroupIdx, chunks.offset.get());
        length->scan(transaction, nodeGroupIdx, chunks.length.get());
    }
    inline void append(
        const CSRHeaderChunks& headerChunks, common::node_group_idx_t nodeGroupIdx) const {
        offset->append(headerChunks.offset.get(), nodeGroupIdx);
        length->append(headerChunks.length.get(), nodeGroupIdx);
    }

    common::offset_t getNumNodes(
        transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx) const;
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

    bool isWithin(const PackedCSRRegion& other) const;

    void setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment);
};

class CSRRelTableData final : public RelTableData {
public:
    struct PersistentState {
        CSRHeaderChunks header;
        std::unique_ptr<ColumnChunk> relIDChunk;
        common::offset_t leftCSROffset = common::INVALID_OFFSET;
        common::offset_t rightCSROffset = common::INVALID_OFFSET;

        explicit PersistentState(common::offset_t numNodes) {
            header = CSRHeaderChunks(false /*enableCompression*/, numNodes);
        }
    };

    struct LocalState {
    public:
        LocalRelNG* localNG;
        CSRHeaderChunks header;
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

        explicit LocalState(LocalRelNG* localNG) : localNG{localNG} { initChangesPerSegment(); }

        inline void setRegion(PackedCSRRegion& region_) { region = region_; }

    private:
        void initChangesPerSegment();
    };

    CSRRelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::RelTableCatalogEntry* relTableEntry, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        RelDataReadState* readState) override;
    void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    bool checkIfNodeHasRels(
        transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector) override;
    void append(NodeGroup* nodeGroup) override;
    void resizeColumns(common::node_group_idx_t numNodeGroups) override;

    inline Column* getCSROffsetColumn() const override { return csrHeaderColumns.offset.get(); }
    inline Column* getCSRLengthColumn() const override { return csrHeaderColumns.length.get(); }

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) override;

    void checkpointInMemory() override;
    void rollbackInMemory() override;

private:
    std::vector<PackedCSRRegion> findRegions(
        const CSRHeaderChunks& headerChunks, LocalState& localState);
    common::length_t getNewRegionSize(const CSRHeaderChunks& header,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region);
    bool isWithinDensityBound(const CSRHeaderChunks& headerChunks,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region);
    double getHighDensity(uint64_t level) const;

    void prepareCommitNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalRelNG* localRelNG);

    void updateCSRHeader(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, PersistentState& persistentState,
        LocalState& localState);
    void distributeOffsets(const CSRHeaderChunks& header, LocalState& localState,
        common::offset_t leftBoundary, common::offset_t rightBoundary);
    void updateRegion(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        PersistentState& persistentState, LocalState& localState);
    void updateColumn(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::column_id_t columnID, const CSRRelTableData::PersistentState& persistentState,
        LocalState& localState);
    void distributeAndUpdateColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::column_id_t columnID,
        const PersistentState& persistentState, LocalState& localState);

    void findPositionsForInsertions(
        common::offset_t nodeOffset, common::length_t numInsertions, LocalState& localState);
    void slideForInsertions(
        common::offset_t nodeOffset, common::length_t numInsertions, LocalState& localState);
    void slideLeftForInsertions(common::offset_t nodeOffset, common::offset_t leftBoundary,
        LocalState& localState, uint64_t numValuesToInsert);
    void slideRightForInsertions(common::offset_t nodeOffset, common::offset_t rightBoundary,
        LocalState& localState, uint64_t numValuesToInsert);

    void applyUpdatesToChunk(const PersistentState& persistentState, const PackedCSRRegion& region,
        LocalVectorCollection* localChunk, const update_insert_info_t& updateInfo,
        ColumnChunk* chunk);
    void applyInsertionsToChunk(const PersistentState& persistentState,
        const LocalState& localState, LocalVectorCollection* localChunk,
        const update_insert_info_t& insertInfo, ColumnChunk* chunk);
    void applyDeletionsToChunk(const PersistentState& persistentState, const LocalState& localState,
        const delete_info_t& deleteInfo, ColumnChunk* chunk);

    void applyUpdatesToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::column_id_t columnID,
        const PersistentState& persistentState, LocalState& localState, Column* column);
    void applyInsertionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::column_id_t columnID, LocalState& localState,
        const PersistentState& persistentState, Column* column);
    void applyDeletionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalState& localState,
        const PersistentState& persistentState, Column* column);
    void applySliding(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalState& localState, const PersistentState& persistentState, Column* column);

    std::vector<std::pair<common::offset_t, common::offset_t>> getSlidesForDeletions(
        const PersistentState& persistentState, const LocalState& localState,
        const delete_info_t& deleteInfo);

    // TODO: Constrain T1 and T2 to numerics.
    template<typename T1, typename T2>
    static uint64_t divideAndRoundUpTo(T1 v1, T2 v2) {
        return std::ceil((double)v1 / (double)v2);
    }
    template<typename T1, typename T2>
    static double divideNoRoundUp(T1 v1, T2 v2) {
        return (double)v1 / (double)v2;
    }
    template<typename T1, typename T2>
    static uint64_t multiplyAndRoundUpTo(T1 v1, T2 v2) {
        return std::ceil((double)v1 * (double)v2);
    }

    LocalVectorCollection* getLocalChunk(
        const LocalState& localState, common::column_id_t columnID);
    Column* getColumn(common::column_id_t columnID) const;

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
};

} // namespace storage
} // namespace kuzu
