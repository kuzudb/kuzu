#pragma once

#include "common/enums/rel_direction.h"
#include "csr_node_group_collection.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

// TODO: Remove this state and also TableDataScanState.
// struct RelDataReadState final : TableDataScanState {
//     common::node_group_idx_t nodeGroupIdx;
//     common::offset_t numNodes;
//     common::offset_t currentNodeOffset;
//     common::offset_t posInCurrentCSR;
//     std::vector<common::list_entry_t> csrListEntries;
//     // Temp auxiliary data structure to scan the offset of each CSR node in the offset column
//     chunk. ChunkedCSRHeader csrHeaderChunks = ChunkedCSRHeader(false /*enableCompression*/,
//         common::StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
//
//     bool readFromPersistentStorage;
//     // Following fields are used for local storage.
//     bool readFromLocalStorage;
//     LocalRelNG* localNodeGroup;
//
//     explicit RelDataReadState(const std::vector<common::column_id_t>& columnIDs);
//     DELETE_COPY_DEFAULT_MOVE(RelDataReadState);
//
//     bool hasMoreToRead(const transaction::Transaction* transaction);
//     void populateCSRListEntries();
//     std::pair<common::offset_t, common::offset_t> getStartAndEndOffset();
//
//     bool hasMoreToReadInPersistentStorage() const;
//
//     bool hasMoreToReadFromLocalStorage() const;
//     bool trySwitchToLocalStorage();
// };

struct CSRHeaderColumns {
    std::unique_ptr<Column> offset;
    std::unique_ptr<Column> length;
};

// TODO(Guodong): Serialize the info to disk. This should be a config per node group.
struct PackedCSRInfo {
    uint64_t calibratorTreeHeight;
    double lowDensityStep;
    double highDensityStep;

    PackedCSRInfo();
};

struct PackedCSRRegion {
    common::idx_t regionIdx = common::INVALID_IDX;
    common::idx_t level = common::INVALID_IDX;
    int64_t sizeChange = 0;
    // Left most and right most node offset of the region.
    common::offset_t leftBoundary = common::INVALID_OFFSET;
    common::offset_t rightBoundary = common::INVALID_OFFSET;

    PackedCSRRegion() {}
    PackedCSRRegion(common::idx_t regionIdx, common::idx_t level);

    std::pair<common::offset_t, common::offset_t> getNodeOffsetBoundaries() const {
        return std::make_pair(leftBoundary, rightBoundary);
    }
    std::pair<common::idx_t, common::idx_t> getSegmentBoundaries() const {
        auto left = regionIdx << level;
        return std::make_pair(left, left + (1 << level) - 1);
    }
    bool isOutOfBoundary(common::offset_t nodeOffset) const {
        return nodeOffset < leftBoundary || nodeOffset > rightBoundary;
    }

    // Check if the other region's segments are all within the range of this region.
    bool isWithin(const PackedCSRRegion& other) const;

    void setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment);
};

class RelsStoreStats;
class RelTableData {
public:
    struct PersistentState {
        ChunkedCSRHeader header;
        std::unique_ptr<ColumnChunkData> relIDChunk;
        common::offset_t leftCSROffset = common::INVALID_OFFSET;
        common::offset_t rightCSROffset = common::INVALID_OFFSET;

        explicit PersistentState(common::offset_t numNodes) {
            header =
                ChunkedCSRHeader(false /*enableCompression*/, numNodes, ResidencyState::IN_MEMORY);
        }
    };

    struct LocalState {
        ChunkedNodeGroup* localNG;
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

        explicit LocalState(ChunkedNodeGroup* localNG);

        void setRegion(const PackedCSRRegion& region_) { region = region_; }
    };

    RelTableData(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal,
        const catalog::TableCatalogEntry* tableEntry, common::RelDataDirection direction,
        bool enableCompression, common::Deserializer* deSer);

    // void initializeScanState(transaction::Transaction* transaction,
    // TableScanState& scanState) const;
    // void scan(transaction::Transaction* transaction, TableDataScanState& readState,
    // common::ValueVector& inNodeIDVector,
    // const std::vector<common::ValueVector*>& outputVectors);
    // void lookup(transaction::Transaction* transaction, TableDataScanState& readState,
    // const common::ValueVector& inNodeIDVector,
    // const std::vector<common::ValueVector*>& outputVectors);

    // dataVectors consist of boundNodeID, internalID, properties, ...
    void append(transaction::Transaction* transaction, common::ValueVector* srcVector,
        std::vector<common::ValueVector*>& dataVectors);

    // TODO: Should be removed. This is used by detachDelete for now.
    bool delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* relIDVector) const;

    void checkRelMultiplicityConstraint(transaction::Transaction* transaction,
        common::ValueVector* srcNodeIDVector) const;
    bool checkIfNodeHasRels(transaction::Transaction* transaction,
        common::offset_t nodeOffset) const;
    common::offset_t append(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkedNodeGroup* nodeGroup);

    Column* getNbrIDColumn() const { return columns[NBR_ID_COLUMN_ID].get(); }
    Column* getCSROffsetColumn() const { return csrHeaderColumns.offset.get(); }
    Column* getCSRLengthColumn() const { return csrHeaderColumns.length.get(); }
    std::vector<Column*> getColumns() const {
        std::vector<Column*> columns;
        columns.reserve(this->columns.size());
        for (const auto& column : this->columns) {
            columns.push_back(column.get());
        }
        return columns;
    }
    CSRNodeGroup& getCSRNodeGroup(common::node_group_idx_t nodeGroupIdx) const {
        return nodeGroups->getNodeGroup(nodeGroupIdx);
    }

    common::row_idx_t getNumRows() const { return nodeGroups->getNumRows(); }

    bool isNewNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const;

    // void prepareLocalTableToCommit(transaction::Transaction* transaction,
    // LocalTableData* localTable);

    void prepareCommitNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkedNodeGroup* localRelNG);

    void serialize(common::Serializer& serializer);

private:
    void initCSRHeaderColumns();
    void initPropertyColumns(const catalog::TableCatalogEntry* tableEntry);

    static common::offset_t getMaxNumNodesInRegion(const ChunkedCSRHeader& header,
        const PackedCSRRegion& region, const ChunkedNodeGroup* localNG);

    // void initializeColumnScanStates(transaction::Transaction* transaction,
    // TableScanState& scanState, common::node_group_idx_t nodeGroupIdx) const;

    std::vector<PackedCSRRegion> findRegions(const ChunkedCSRHeader& headerChunks,
        LocalState& localState) const;
    static common::length_t getNewRegionSize(const ChunkedCSRHeader& header,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region);
    bool isWithinDensityBound(const ChunkedCSRHeader& headerChunks,
        const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region) const;
    double getHighDensity(uint64_t level) const;

    void updateCSRHeader(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        PersistentState& persistentState, LocalState& localState);
    static void commitCSRHeaderChunk(transaction::Transaction* transaction, bool isNewNodeGroup,
        common::node_group_idx_t nodeGroupIdx, Column* column, ColumnChunkData* columnChunk,
        const LocalState& localState, const std::vector<common::offset_t>& dstOffsets);

    void distributeOffsets(const ChunkedCSRHeader& header, LocalState& localState,
        common::offset_t leftBoundary, common::offset_t rightBoundary) const;
    void updateRegion(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, PersistentState& persistentState, LocalState& localState);
    void updateColumn(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, common::column_id_t columnID, const PersistentState& persistentState,
        LocalState& localState);
    void distributeAndUpdateColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        const PersistentState& persistentState, const LocalState& localState) const;

    static void findPositionsForInsertions(common::offset_t nodeOffset,
        common::length_t numInsertions, LocalState& localState);
    static void slideForInsertions(common::offset_t nodeOffset, common::length_t numInsertions,
        const LocalState& localState);
    static void slideLeftForInsertions(common::offset_t nodeOffset, common::offset_t leftBoundary,
        const LocalState& localState, uint64_t numValuesToInsert);
    static void slideRightForInsertions(common::offset_t nodeOffset, common::offset_t rightBoundary,
        const LocalState& localState, uint64_t numValuesToInsert);

    static void applyUpdatesToChunk(const PersistentState& persistentState,
        const LocalState& localState, const ChunkDataCollection& localChunk, ColumnChunkData* chunk,
        common::column_id_t columnID);
    static void applyInsertionsToChunk(const PersistentState& persistentState,
        const LocalState& localState, const ChunkDataCollection& localChunk,
        ColumnChunkData* chunk);
    static void applyDeletionsToChunk(const PersistentState& persistentState,
        const LocalState& localState, ColumnChunkData* chunk);

    static void applyUpdatesToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        const PersistentState& persistentState, const LocalState& localState, Column* column);
    static void applyInsertionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, common::column_id_t columnID,
        const LocalState& localState, const PersistentState& persistentState, Column* column);
    void applyDeletionsToColumn(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, const LocalState& localState,
        const PersistentState& persistentState, Column* column) const;
    void applySliding(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, const LocalState& localState, const PersistentState& persistentState,
        Column* column) const;

    static std::vector<std::pair<common::offset_t, common::offset_t>> getSlidesForDeletions(
        const PersistentState& persistentState, const LocalState& localState);

    // ChunkedNodeGroup* getLocalNodeGroup(transaction::Transaction* transaction,
    // common::node_group_idx_t nodeGroupIdx) const;

    template<typename T1, typename T2>
    static double divideNoRoundUp(T1 v1, T2 v2) {
        static_assert(std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2>);
        return static_cast<double>(v1) / static_cast<double>(v2);
    }
    template<typename T1, typename T2>
    static uint64_t multiplyAndRoundUpTo(T1 v1, T2 v2) {
        static_assert(std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2>);
        return std::ceil(static_cast<double>(v1) * static_cast<double>(v2));
    }

    static void fillSequence(std::span<common::offset_t> offsets, common::offset_t startOffset) {
        for (auto i = 0u; i < offsets.size(); i++) {
            offsets[i] = i + startOffset;
        }
    }

    static common::offset_t findCSROffsetInRegion(const PersistentState& persistentState,
        common::offset_t nodeOffset, common::offset_t relOffset);

private:
    std::vector<common::LogicalType> getColumnTypes() const {
        std::vector<common::LogicalType> types;
        types.reserve(columns.size());
        for (const auto& column : columns) {
            types.push_back(column->getDataType());
        }
        return types;
    }

private:
    BMFileHandle* dataFH;
    common::table_id_t tableID;
    std::string tableName;
    BufferManager* bufferManager;
    WAL* wal;
    bool enableCompression;
    PackedCSRInfo packedCSRInfo;
    common::RelDataDirection direction;
    common::RelMultiplicity multiplicity;
    CSRHeaderColumns csrHeaderColumns;
    std::unique_ptr<CSRNodeGroupCollection> nodeGroups;
    std::vector<std::unique_ptr<Column>> columns;
};

} // namespace storage
} // namespace kuzu
