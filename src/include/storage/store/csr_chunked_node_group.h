#pragma once

#include <algorithm>

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace storage {

struct CSRRegion {
    common::idx_t regionIdx = common::INVALID_IDX;
    common::idx_t level = common::INVALID_IDX;
    common::offset_t leftNodeOffset = common::INVALID_OFFSET;
    common::offset_t rightNodeOffset = common::INVALID_OFFSET;
    int64_t sizeChange = 0;
    // Track if there is any updates to persistent data in this region per column in table.
    // Note: should be accessed with columnID.
    std::vector<bool> hasUpdates;
    // Note: `sizeChange` equal to 0 is not enough to indicate the region has no insert or
    // delete. It might just be num of insertions are equal to num of deletions.
    // hasInsertions is true if there are insertions that are not deleted yet in this region.
    bool hasInsertions = false;
    bool hasPersistentDeletions = false;

    CSRRegion(common::idx_t regionIdx, common::idx_t level);

    bool needCheckpoint() const {
        return hasInsertions || hasPersistentDeletions ||
               std::any_of(hasUpdates.begin(), hasUpdates.end(),
                   [](bool hasUpdate) { return hasUpdate; });
    }
    bool needCheckpointColumn(common::column_id_t columnID) const {
        KU_ASSERT(columnID < hasUpdates.size());
        return hasInsertions || hasPersistentDeletions || hasUpdates[columnID];
    }
    bool hasDeletionsOrInsertions() const { return hasInsertions || hasPersistentDeletions; }
    common::idx_t getLeftLeafRegionIdx() const { return regionIdx << level; }
    common::idx_t getRightLeafRegionIdx() const {
        const auto rightRegionIdx =
            getLeftLeafRegionIdx() + (static_cast<common::idx_t>(1) << level) - 1;
        constexpr auto maxNumRegions = common::StorageConstants::NODE_GROUP_SIZE /
                                       common::StorageConstants::CSR_LEAF_REGION_SIZE;
        if (rightRegionIdx >= maxNumRegions) {
            return maxNumRegions - 1;
        }
        return rightRegionIdx;
    }
    // Return true if other is within the realm of this region.
    bool isWithin(const CSRRegion& other) const;

    static CSRRegion upgradeLevel(const std::vector<CSRRegion>& leafRegions,
        const CSRRegion& region);
};

struct ChunkedCSRHeader {
    std::unique_ptr<ColumnChunk> offset;
    std::unique_ptr<ColumnChunk> length;

    ChunkedCSRHeader(bool enableCompression, uint64_t capacity, ResidencyState residencyState);
    ChunkedCSRHeader(std::unique_ptr<ColumnChunk> offset, std::unique_ptr<ColumnChunk> length)
        : offset{std::move(offset)}, length{std::move(length)} {}

    common::offset_t getStartCSROffset(common::offset_t nodeOffset) const;
    common::offset_t getEndCSROffset(common::offset_t nodeOffset) const;
    common::length_t getCSRLength(common::offset_t nodeOffset) const;
    common::length_t getGapSize(common::length_t length) const;

    bool sanityCheck() const;
    void copyFrom(const ChunkedCSRHeader& other) const;
    void fillDefaultValues(common::offset_t newNumValues) const;
    void setNumValues(const common::offset_t numValues) const {
        offset->setNumValues(numValues);
        length->setNumValues(numValues);
    }

    void resetToEmpty() const {
        offset->resetToEmpty();
        length->resetToEmpty();
    }

    // Return a vector of CSR offsets for the end of each CSR region.
    std::vector<common::offset_t> populateStartCSROffsetsFromLength(bool leaveGaps) const;
    void populateEndCSROffsetFromStartAndLength() const;
    void finalizeCSRRegionEndOffsets(
        const std::vector<common::offset_t>& rightCSROffsetOfRegions) const;
    void populateRegionCSROffsets(const CSRRegion& region, const ChunkedCSRHeader& oldHeader) const;
    void populateEndCSROffsets(const std::vector<common::offset_t>& gaps) const;
    common::idx_t getNumRegions() const;

private:
    static common::length_t computeGapFromLength(common::length_t length);
};

struct CSRNodeGroupCheckpointState;
class ChunkedCSRNodeGroup final : public ChunkedNodeGroup {
public:
    ChunkedCSRNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity, common::offset_t startOffset, ResidencyState residencyState)
        : ChunkedNodeGroup{columnTypes, enableCompression, capacity, startOffset, residencyState,
              NodeGroupDataFormat::CSR},
          csrHeader{enableCompression, common::StorageConstants::NODE_GROUP_SIZE, residencyState} {}
    ChunkedCSRNodeGroup(ChunkedCSRNodeGroup& base,
        const std::vector<common::column_id_t>& selectedColumns)
        : ChunkedNodeGroup{base, selectedColumns}, csrHeader{std::move(base.csrHeader)} {}
    ChunkedCSRNodeGroup(ChunkedCSRHeader csrHeader,
        std::vector<std::unique_ptr<ColumnChunk>> chunks, common::row_idx_t startRowIdx)
        : ChunkedNodeGroup{std::move(chunks), startRowIdx, NodeGroupDataFormat::CSR},
          csrHeader{std::move(csrHeader)} {}

    ChunkedCSRHeader& getCSRHeader() { return csrHeader; }
    const ChunkedCSRHeader& getCSRHeader() const { return csrHeader; }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<ChunkedCSRNodeGroup> deserialize(common::Deserializer& deSer);

    void writeToColumnChunk(common::idx_t chunkIdx, common::idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) override {
        chunks[chunkIdx]->getData().write(&data[vectorIdx]->getData(), &offsetChunk.getData(),
            common::RelMultiplicity::MANY);
    }

    void scanCSRHeader(CSRNodeGroupCheckpointState& csrState) const;

    std::unique_ptr<ChunkedNodeGroup> flushAsNewChunkedNodeGroup(
        transaction::Transaction* transaction, BMFileHandle& dataFH) const override;

    void flush(BMFileHandle& dataFH) override;

private:
    ChunkedCSRHeader csrHeader;
};

} // namespace storage
} // namespace kuzu
