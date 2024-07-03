#include "storage/store/csr_chunked_node_group.h"

#include "common/serializer/deserializer.h"
#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ChunkedCSRHeader::ChunkedCSRHeader(bool enableCompression, uint64_t capacity,
    ResidencyState residencyState) {
    offset = std::make_unique<ColumnChunk>(LogicalType::UINT64(), capacity, enableCompression,
        residencyState);
    length = std::make_unique<ColumnChunk>(LogicalType::UINT64(), capacity, enableCompression,
        residencyState);
}

offset_t ChunkedCSRHeader::getStartCSROffset(offset_t nodeOffset) const {
    // TODO(Guodong): I think we can simplify the check here by getting rid of some of the
    // conditions.
    if (nodeOffset == 0 || offset->getNumValues() == 0) {
        return 0;
    }
    if (nodeOffset >= offset->getNumValues()) {
        return offset->getNumValues() - 1;
    }
    KU_ASSERT(nodeOffset < offset->getNumValues());
    return offset->getData().getValue<offset_t>(nodeOffset - 1);
}

offset_t ChunkedCSRHeader::getEndCSROffset(offset_t nodeOffset) const {
    // TODO(Guodong): I think we can simplify the check here by getting rid of some of the
    // conditions.
    if (offset->getNumValues() == 0) {
        return 0;
    }
    if (nodeOffset >= offset->getNumValues()) {
        return offset->getNumValues() - 1;
    }
    KU_ASSERT(nodeOffset < offset->getNumValues());
    KU_ASSERT(nodeOffset < length->getNumValues());
    return offset->getData().getValue<offset_t>(nodeOffset);
}

length_t ChunkedCSRHeader::getCSRLength(offset_t nodeOffset) const {
    if (nodeOffset >= offset->getNumValues()) {
        return 0;
    }
    KU_ASSERT(nodeOffset < length->getNumValues());
    return length->getData().getValue<length_t>(nodeOffset);
}

bool ChunkedCSRHeader::sanityCheck() const {
    if (offset->getNumValues() != length->getNumValues()) {
        return false;
    }
    if (offset->getNumValues() == 0) {
        return true;
    }
    if (offset->getData().getValue<offset_t>(0) < length->getData().getValue<length_t>(0)) {
        return false;
    }
    for (auto i = 1u; i < offset->getNumValues(); i++) {
        if (offset->getData().getValue<offset_t>(i - 1) + length->getData().getValue<length_t>(i) >
            offset->getData().getValue<offset_t>(i)) {
            return false;
        }
    }
    return true;
}

void ChunkedCSRHeader::copyFrom(const ChunkedCSRHeader& other) const {
    const auto numValues = other.offset->getNumValues();
    memcpy(offset->getData().getData(), other.offset->getData().getData(),
        numValues * sizeof(offset_t));
    memcpy(length->getData().getData(), other.length->getData().getData(),
        numValues * sizeof(length_t));
    length->setNumValues(numValues);
    offset->setNumValues(numValues);
}

void ChunkedCSRHeader::fillDefaultValues(const offset_t newNumValues) const {
    const auto lastCSROffset = getEndCSROffset(length->getNumValues() - 1);
    for (auto i = length->getNumValues(); i < newNumValues; i++) {
        offset->getData().setValue<offset_t>(lastCSROffset, i);
        length->getData().setValue<length_t>(0, i);
    }
    KU_ASSERT(
        offset->getNumValues() >= newNumValues && length->getNumValues() == offset->getNumValues());
}

std::vector<offset_t> ChunkedCSRHeader::populateStartCSROffsetsAndGaps(bool leaveGaps) {
    KU_ASSERT(length->getNumValues() == offset->getNumValues());
    std::vector<offset_t> gaps;
    const auto numNodes = length->getNumValues();
    gaps.resize(numNodes, 0);
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    const auto csrLengths = reinterpret_cast<length_t*>(length->getData().getData());
    // Calculate gaps for each node.
    if (leaveGaps) {
        for (auto i = 0u; i < numNodes; i++) {
            gaps[i] = getGapSize(csrLengths[i]);
        }
    }
    csrOffsets[0] = 0;
    // Calculate starting offset of each node.
    for (auto i = 1u; i < numNodes; i++) {
        csrOffsets[i] = csrOffsets[i - 1] + csrLengths[i - 1] + gaps[i - 1];
    }
    return gaps;
}

void ChunkedCSRHeader::populateEndCSROffsets(const std::vector<offset_t>& gaps) {
    auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    KU_ASSERT(offset->getNumValues() == length->getNumValues());
    KU_ASSERT(offset->getNumValues() == gaps.size());
    for (auto i = 0u; i < offset->getNumValues(); i++) {
        csrOffsets[i] += gaps[i];
    }
}

length_t ChunkedCSRHeader::getGapSize(length_t length) {
    // We intentionally leave a gap for empty CSR lists to accommondate for future insertions.
    // Also, for MANY_ONE and ONE_ONE relationships, we should always keep each CSR list as size 1.
    return length == 0 ?
               1 :
               StorageUtils::divideAndRoundUpTo(length, StorageConstants::PACKED_CSR_DENSITY) -
                   length;
}

std::unique_ptr<ChunkedNodeGroup> ChunkedCSRNodeGroup::flushAsNewChunkedNodeGroup(
    BMFileHandle& dataFH) const {
    auto csrOffset = std::make_unique<ColumnChunk>(csrHeader.offset->isCompressionEnabled(),
        Column::flushChunkData(csrHeader.offset->getData(), dataFH));
    auto csrLength = std::make_unique<ColumnChunk>(csrHeader.length->isCompressionEnabled(),
        Column::flushChunkData(csrHeader.length->getData(), dataFH));
    std::vector<std::unique_ptr<ColumnChunk>> flushedChunks(getNumColumns());
    for (auto i = 0u; i < getNumColumns(); i++) {
        flushedChunks[i] = std::make_unique<ColumnChunk>(getColumnChunk(i).isCompressionEnabled(),
            Column::flushChunkData(getColumnChunk(i).getData(), dataFH));
    }
    ChunkedCSRHeader csrHeader{std::move(csrOffset), std::move(csrLength)};
    return std::make_unique<ChunkedCSRNodeGroup>(std::move(csrHeader), std::move(flushedChunks),
        0 /*startRowIdx*/);
}

void ChunkedCSRNodeGroup::flush(BMFileHandle& dataFH) {
    csrHeader.offset->getData().flush(dataFH);
    csrHeader.length->getData().flush(dataFH);
    for (auto i = 0u; i < getNumColumns(); i++) {
        getColumnChunk(i).getData().flush(dataFH);
    }
}

void ChunkedCSRNodeGroup::scanCSRHeader(CSRNodeGroupCheckpointState& csrState) const {
    if (!csrState.oldHeader) {
        csrState.oldHeader = std::make_unique<ChunkedCSRHeader>(false /*enableCompression*/,
            StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    }
    ChunkState headerChunkState;
    KU_ASSERT(csrHeader.offset->getResidencyState() == ResidencyState::ON_DISK);
    KU_ASSERT(csrHeader.length->getResidencyState() == ResidencyState::ON_DISK);
    csrHeader.offset->initializeScanState(headerChunkState);
    KU_ASSERT(csrState.csrOffsetColumn && csrState.csrLengthColumn);
    csrState.csrOffsetColumn->scan(&transaction::DUMMY_CHECKPOINT_TRANSACTION, headerChunkState,
        &csrState.oldHeader->offset->getData());
    csrHeader.length->initializeScanState(headerChunkState);
    csrState.csrLengthColumn->scan(&transaction::DUMMY_CHECKPOINT_TRANSACTION, headerChunkState,
        &csrState.oldHeader->length->getData());
}

void ChunkedCSRNodeGroup::serialize(Serializer& serializer) const {
    KU_ASSERT(csrHeader.offset && csrHeader.length);
    csrHeader.offset->serialize(serializer);
    csrHeader.length->serialize(serializer);
    ChunkedNodeGroup::serialize(serializer);
}

std::unique_ptr<ChunkedCSRNodeGroup> ChunkedCSRNodeGroup::deserialize(Deserializer& deSer) {
    auto offset = ColumnChunk::deserialize(deSer);
    auto length = ColumnChunk::deserialize(deSer);
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks);
    auto chunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(
        ChunkedCSRHeader{std::move(offset), std::move(length)}, std::move(chunks),
        0 /*startRowIdx*/);
    bool hasVersions;
    deSer.deserializeValue<bool>(hasVersions);
    if (hasVersions) {
        chunkedGroup->versionInfo = VersionInfo::deserialize(deSer);
    }
    return chunkedGroup;
}

} // namespace storage
} // namespace kuzu
