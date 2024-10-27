#include "storage/store/csr_chunked_node_group.h"

#include "common/serializer/deserializer.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/column.h"
#include "storage/store/csr_node_group.h"
#include "transaction/transaction.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

CSRRegion::CSRRegion(idx_t regionIdx, idx_t level) : regionIdx{regionIdx}, level{level} {
    const auto leftLeafRegion = regionIdx << level;
    leftNodeOffset = leftLeafRegion << StorageConstants::CSR_LEAF_REGION_SIZE_LOG2;
    rightNodeOffset = leftNodeOffset + (StorageConstants::CSR_LEAF_REGION_SIZE << level) - 1;
    if (rightNodeOffset >= StorageConstants::NODE_GROUP_SIZE) {
        // The max right node offset should be NODE_GROUP_SIZE - 1.
        rightNodeOffset = StorageConstants::NODE_GROUP_SIZE - 1;
    }
}

bool CSRRegion::isWithin(const CSRRegion& other) const {
    if (other.level <= level) {
        return false;
    }
    const auto leftRegionIdx = getLeftLeafRegionIdx();
    const auto rightRegionIdx = getRightLeafRegionIdx();
    const auto otherLeftRegionIdx = other.getLeftLeafRegionIdx();
    const auto otherRightRegionIdx = other.getRightLeafRegionIdx();
    return leftRegionIdx >= otherLeftRegionIdx && rightRegionIdx <= otherRightRegionIdx;
}

CSRRegion CSRRegion::upgradeLevel(const std::vector<CSRRegion>& leafRegions,
    const CSRRegion& region) {
    const auto regionIdx = region.regionIdx >> 1;
    CSRRegion newRegion{regionIdx, region.level + 1};
    newRegion.hasUpdates.resize(region.hasUpdates.size(), false);
    const idx_t leftLeafRegionIdx = newRegion.getLeftLeafRegionIdx();
    const idx_t rightLeafRegionIdx = newRegion.getRightLeafRegionIdx();
    for (auto leafRegionIdx = leftLeafRegionIdx; leafRegionIdx <= rightLeafRegionIdx;
         leafRegionIdx++) {
        KU_ASSERT(leafRegionIdx < leafRegions.size());
        newRegion.sizeChange += leafRegions[leafRegionIdx].sizeChange;
        newRegion.hasPersistentDeletions |= leafRegions[leafRegionIdx].hasPersistentDeletions;
        newRegion.hasInsertions |= leafRegions[leafRegionIdx].hasInsertions;
        for (auto columnID = 0u; columnID < leafRegions[leafRegionIdx].hasUpdates.size();
             columnID++) {
            newRegion.hasUpdates[columnID] =
                static_cast<bool>(newRegion.hasUpdates[columnID]) ||
                static_cast<bool>(leafRegions[leafRegionIdx].hasUpdates[columnID]);
        }
    }
    return newRegion;
}

ChunkedCSRHeader::ChunkedCSRHeader(MemoryManager& memoryManager, bool enableCompression,
    uint64_t capacity, ResidencyState residencyState) {
    offset = std::make_unique<ColumnChunk>(memoryManager, LogicalType::UINT64(), capacity,
        enableCompression, residencyState, false);
    length = std::make_unique<ColumnChunk>(memoryManager, LogicalType::UINT64(), capacity,
        enableCompression, residencyState, false);
}

offset_t ChunkedCSRHeader::getStartCSROffset(offset_t nodeOffset) const {
    // TODO(Guodong): I think we can simplify the check here by getting rid of some of the
    // conditions.
    auto numValues = offset->getNumValues();
    if (nodeOffset == 0 || numValues == 0) {
        return 0;
    }
    return offset->getData().getValue<offset_t>(
        nodeOffset >= numValues ? (numValues - 1) : nodeOffset - 1);
}

offset_t ChunkedCSRHeader::getEndCSROffset(offset_t nodeOffset) const {
    // TODO(Guodong): I think we can simplify the check here by getting rid of some of the
    // conditions.
    auto numValues = offset->getNumValues();
    if (numValues == 0) {
        return 0;
    }
    return offset->getData().getValue<offset_t>(
        nodeOffset >= numValues ? (numValues - 1) : nodeOffset);
}

length_t ChunkedCSRHeader::getCSRLength(offset_t nodeOffset) const {
    if (nodeOffset >= offset->getNumValues()) {
        return 0;
    }
    KU_ASSERT(nodeOffset < length->getNumValues());
    return length->getData().getValue<length_t>(nodeOffset);
}

length_t ChunkedCSRHeader::getGapSize(offset_t nodeOffset) const {
    return getEndCSROffset(nodeOffset) - getStartCSROffset(nodeOffset) - getCSRLength(nodeOffset);
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

std::vector<offset_t> ChunkedCSRHeader::populateStartCSROffsetsFromLength(bool leaveGaps) const {
    const auto numNodes = length->getNumValues();
    const auto numLeafRegions = getNumRegions();
    offset_t leftCSROffset = 0;
    std::vector<offset_t> rightCSROffsetOfRegions;
    rightCSROffsetOfRegions.reserve(numLeafRegions);
    for (auto regionIdx = 0u; regionIdx < numLeafRegions; regionIdx++) {
        CSRRegion region{regionIdx, 0 /* level*/};
        length_t numRelsInRegion = 0;
        const auto rightNodeOffset = std::min(region.rightNodeOffset, numNodes - 1);
        // Populate start csr offset for each node in the region.
        for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= rightNodeOffset; nodeOffset++) {
            offset->getData().setValue<offset_t>(leftCSROffset + numRelsInRegion, nodeOffset);
            numRelsInRegion += getCSRLength(nodeOffset);
        }
        // Update lastLeftCSROffset for next region.
        leftCSROffset += numRelsInRegion;
        if (leaveGaps) {
            leftCSROffset += computeGapFromLength(numRelsInRegion);
        }
        rightCSROffsetOfRegions.push_back(leftCSROffset);
    }
    return rightCSROffsetOfRegions;
}

void ChunkedCSRHeader::populateEndCSROffsetFromStartAndLength() const {
    const auto numNodes = length->getNumValues();
    KU_ASSERT(offset->getNumValues() == numNodes);
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    const auto csrLengths = reinterpret_cast<length_t*>(length->getData().getData());
    for (auto i = 0u; i < numNodes; i++) {
        csrOffsets[i] = csrOffsets[i] + csrLengths[i];
    }
}

void ChunkedCSRHeader::finalizeCSRRegionEndOffsets(
    const std::vector<offset_t>& rightCSROffsetOfRegions) const {
    const auto numNodes = length->getNumValues();
    const auto numLeafRegions = getNumRegions();
    KU_ASSERT(numLeafRegions == rightCSROffsetOfRegions.size());
    for (auto regionIdx = 0u; regionIdx < numLeafRegions; regionIdx++) {
        CSRRegion region{regionIdx, 0 /* level*/};
        const auto rightNodeOffset = std::min(region.rightNodeOffset, numNodes - 1);
        offset->getData().setValue<offset_t>(rightCSROffsetOfRegions[regionIdx], rightNodeOffset);
    }
}

idx_t ChunkedCSRHeader::getNumRegions() const {
    const auto numNodes = length->getNumValues();
    KU_ASSERT(offset->getNumValues() == numNodes);
    return (numNodes + StorageConstants::CSR_LEAF_REGION_SIZE - 1) /
           StorageConstants::CSR_LEAF_REGION_SIZE;
}

void ChunkedCSRHeader::populateRegionCSROffsets(const CSRRegion& region,
    const ChunkedCSRHeader& oldHeader) const {
    KU_ASSERT(region.level <= CSRNodeGroup::DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight);
    const auto leftNodeOffset = region.leftNodeOffset;
    const auto rightNodeOffset = region.rightNodeOffset;
    const auto leftCSROffset = oldHeader.getStartCSROffset(leftNodeOffset);
    const auto oldRightCSROffset = oldHeader.getEndCSROffset(rightNodeOffset);
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    const auto csrLengths = reinterpret_cast<length_t*>(length->getData().getData());
    length_t numRelsInRegion = 0u;
    for (auto i = leftNodeOffset; i <= rightNodeOffset; i++) {
        numRelsInRegion += csrLengths[i];
        csrOffsets[i] = leftCSROffset + numRelsInRegion;
    }
    // We should keep the region stable and the old right CSR offset is the end of the region.
    KU_ASSERT(csrOffsets[rightNodeOffset] <= oldRightCSROffset);
    csrOffsets[rightNodeOffset] = oldRightCSROffset;
}

void ChunkedCSRHeader::populateEndCSROffsets(const std::vector<offset_t>& gaps) const {
    const auto csrOffsets = reinterpret_cast<offset_t*>(offset->getData().getData());
    KU_ASSERT(offset->getNumValues() == length->getNumValues());
    KU_ASSERT(offset->getNumValues() == gaps.size());
    for (auto i = 0u; i < offset->getNumValues(); i++) {
        csrOffsets[i] += gaps[i];
    }
}

length_t ChunkedCSRHeader::computeGapFromLength(length_t length) {
    return StorageUtils::divideAndRoundUpTo(length, StorageConstants::PACKED_CSR_DENSITY) - length;
}

std::unique_ptr<ChunkedNodeGroup> ChunkedCSRNodeGroup::flushAsNewChunkedNodeGroup(
    transaction::Transaction* transaction, FileHandle& dataFH) const {
    auto csrOffset = std::make_unique<ColumnChunk>(csrHeader.offset->isCompressionEnabled(),
        Column::flushChunkData(csrHeader.offset->getData(), dataFH));
    auto csrLength = std::make_unique<ColumnChunk>(csrHeader.length->isCompressionEnabled(),
        Column::flushChunkData(csrHeader.length->getData(), dataFH));
    std::vector<std::unique_ptr<ColumnChunk>> flushedChunks(getNumColumns());
    for (auto i = 0u; i < getNumColumns(); i++) {
        flushedChunks[i] = std::make_unique<ColumnChunk>(getColumnChunk(i).isCompressionEnabled(),
            Column::flushChunkData(getColumnChunk(i).getData(), dataFH));
    }
    ChunkedCSRHeader newCSRHeader{std::move(csrOffset), std::move(csrLength)};
    auto flushedChunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(std::move(newCSRHeader),
        std::move(flushedChunks), 0 /*startRowIdx*/);
    flushedChunkedGroup->versionInfo = std::make_unique<VersionInfo>();
    KU_ASSERT(numRows == flushedChunkedGroup->getNumRows());
    flushedChunkedGroup->versionInfo->append(transaction, flushedChunkedGroup.get(), 0, numRows);
    return flushedChunkedGroup;
}

void ChunkedCSRNodeGroup::flush(FileHandle& dataFH) {
    csrHeader.offset->getData().flush(dataFH);
    csrHeader.length->getData().flush(dataFH);
    for (auto i = 0u; i < getNumColumns(); i++) {
        getColumnChunk(i).getData().flush(dataFH);
    }
}

void ChunkedCSRNodeGroup::scanCSRHeader(MemoryManager& memoryManager,
    CSRNodeGroupCheckpointState& csrState) const {
    if (!csrState.oldHeader) {
        csrState.oldHeader =
            std::make_unique<ChunkedCSRHeader>(memoryManager, false /*enableCompression*/,
                StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    }
    ChunkState headerChunkState;
    KU_ASSERT(csrHeader.offset->getResidencyState() == ResidencyState::ON_DISK);
    KU_ASSERT(csrHeader.length->getResidencyState() == ResidencyState::ON_DISK);
    csrHeader.offset->initializeScanState(headerChunkState, csrState.csrOffsetColumn);
    KU_ASSERT(csrState.csrOffsetColumn && csrState.csrLengthColumn);
    csrState.csrOffsetColumn->scan(&transaction::DUMMY_CHECKPOINT_TRANSACTION, headerChunkState,
        &csrState.oldHeader->offset->getData());
    csrHeader.length->initializeScanState(headerChunkState, csrState.csrLengthColumn);
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

std::unique_ptr<ChunkedCSRNodeGroup> ChunkedCSRNodeGroup::deserialize(MemoryManager& memoryManager,
    Deserializer& deSer) {
    std::string key;
    deSer.validateDebuggingInfo(key, "csr_header_offset");
    auto offset = ColumnChunk::deserialize(memoryManager, deSer);
    deSer.validateDebuggingInfo(key, "csr_header_length");
    auto length = ColumnChunk::deserialize(memoryManager, deSer);
    // TODO(Guodong): Rework to reuse ChunkedNodeGroup::deserialize().
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    deSer.validateDebuggingInfo(key, "chunks");
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks,
        [&](Deserializer& deser) { return ColumnChunk::deserialize(memoryManager, deser); });
    auto chunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(
        ChunkedCSRHeader{std::move(offset), std::move(length)}, std::move(chunks),
        0 /*startRowIdx*/);
    bool hasVersions = false;
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
