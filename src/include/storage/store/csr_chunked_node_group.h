#pragma once

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace storage {

struct ChunkedCSRHeader {
    std::unique_ptr<ColumnChunk> offset;
    std::unique_ptr<ColumnChunk> length;

    ChunkedCSRHeader(bool enableCompression, uint64_t capacity, ResidencyState residencyState);
    ChunkedCSRHeader(std::unique_ptr<ColumnChunk> offset, std::unique_ptr<ColumnChunk> length)
        : offset{std::move(offset)}, length{std::move(length)} {}

    common::offset_t getStartCSROffset(common::offset_t nodeOffset) const;
    common::offset_t getEndCSROffset(common::offset_t nodeOffset) const;
    common::length_t getCSRLength(common::offset_t nodeOffset) const;

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

    void populateCSROffsets() const;
    std::vector<common::offset_t> populateStartCSROffsetsAndGaps(bool leaveGaps) const;
    void populateEndCSROffsets(const std::vector<common::offset_t>& gaps) const;

private:
    static common::length_t getGapSize(common::length_t length);
};

struct CSRNodeGroupCheckpointState;
class ChunkedCSRNodeGroup final : public ChunkedNodeGroup {
public:
    ChunkedCSRNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity, common::offset_t startOffset, ResidencyState residencyState)
        : ChunkedNodeGroup{columnTypes, enableCompression, capacity, startOffset, residencyState,
              NodeGroupDataFormat::CSR},
          csrHeader{enableCompression, common::StorageConstants::NODE_GROUP_SIZE, residencyState} {}
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
