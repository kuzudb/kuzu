#include "storage/store/csr_chunked_node_group.h"

#include "common/serializer/deserializer.h"
#include "storage/storage_utils.h"
#include "storage/store/column.h"
#include "storage/store/csr_node_group.h"
#include "transaction/transaction.h"

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
    KU_ASSERT(offset->getNumValues() == length->getNumValues());
    KU_ASSERT(other.offset->getNumValues() == other.length->getNumValues());
    const auto numOtherValues = other.offset->getNumValues();
    memcpy(offset->getData().getData(), other.offset->getData().getData(),
        numOtherValues * sizeof(offset_t));
    memcpy(length->getData().getData(), other.length->getData().getData(),
        numOtherValues * sizeof(length_t));
    const auto lastOffsetInOtherHeader = other.getEndCSROffset(numOtherValues);
    const auto numValues = offset->getNumValues();
    for (auto i = numOtherValues; i < numValues; i++) {
        offset->getData().setValue<offset_t>(lastOffsetInOtherHeader, i);
        length->getData().setValue<length_t>(0, i);
    }
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

void ChunkedCSRHeader::populateCSROffsets() const {
    const auto numNodes = length->getNumValues();
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    const auto csrLengths = reinterpret_cast<length_t*>(length->getData().getData());
    csrOffsets[0] = csrLengths[0] + getGapSize(csrLengths[0]);
    // Calculate starting offset of each node.
    for (auto i = 1u; i < numNodes; i++) {
        const auto gap = getGapSize(csrLengths[i]);
        csrOffsets[i] = csrOffsets[i - 1] + csrLengths[i] + gap;
    }
}

std::vector<offset_t> ChunkedCSRHeader::populateStartCSROffsetsAndGaps(bool leaveGaps) const {
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

void ChunkedCSRHeader::populateEndCSROffsets(const std::vector<offset_t>& gaps) const {
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    KU_ASSERT(offset->getNumValues() == length->getNumValues());
    KU_ASSERT(offset->getNumValues() == gaps.size());
    for (auto i = 0u; i < offset->getNumValues(); i++) {
        csrOffsets[i] += gaps[i];
    }
}

length_t ChunkedCSRHeader::getGapSize(length_t length) {
    return StorageUtils::divideAndRoundUpTo(length, StorageConstants::PACKED_CSR_DENSITY) - length;
}

std::unique_ptr<ChunkedNodeGroup> ChunkedCSRNodeGroup::flushAsNewChunkedNodeGroup(
    transaction::Transaction* transaction, BMFileHandle& dataFH) const {
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
    auto flushedChunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(std::move(csrHeader),
        std::move(flushedChunks), 0 /*startRowIdx*/);
    flushedChunkedGroup->versionInfo = std::make_unique<VersionInfo>();
    KU_ASSERT(numRows == flushedChunkedGroup->getNumRows());
    flushedChunkedGroup->versionInfo->append(transaction, 0, numRows);
    return flushedChunkedGroup;
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
    serializer.writeDebuggingInfo("csr_header_offset");
    csrHeader.offset->serialize(serializer);
    serializer.writeDebuggingInfo("csr_header_length");
    csrHeader.length->serialize(serializer);
    ChunkedNodeGroup::serialize(serializer);
}

std::unique_ptr<ChunkedCSRNodeGroup> ChunkedCSRNodeGroup::deserialize(Deserializer& deSer) {
    std::string key;
    deSer.validateDebuggingInfo(key, "csr_header_offset");
    auto offset = ColumnChunk::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "csr_header_length");
    auto length = ColumnChunk::deserialize(deSer);
    // TODO(Guodong): Rework to reuse ChunkedNodeGroup::deserialize().
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    deSer.validateDebuggingInfo(key, "chunks");
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks);
    auto chunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(
        ChunkedCSRHeader{std::move(offset), std::move(length)}, std::move(chunks),
        0 /*startRowIdx*/);
    bool hasVersions;
    deSer.validateDebuggingInfo(key, "has_version_info");
    deSer.deserializeValue<bool>(hasVersions);
    if (hasVersions) {
        deSer.validateDebuggingInfo(key, "version_info");
        chunkedGroup->versionInfo = VersionInfo::deserialize(deSer);
    }
    return chunkedGroup;
}

} // namespace storage
} // namespace kuzu
