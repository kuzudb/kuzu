#include "storage/store/csr_chunked_node_group.h"

#include "common/serializer/deserializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ChunkedCSRHeader::ChunkedCSRHeader(bool enableCompression, uint64_t capacity,
    ResidencyState residencyState) {
    offset = std::make_unique<ColumnChunk>(*LogicalType::UINT64(), capacity, enableCompression,
        residencyState);
    length = std::make_unique<ColumnChunk>(*LogicalType::UINT64(), capacity, enableCompression,
        residencyState);
}

offset_t ChunkedCSRHeader::getStartCSROffset(offset_t nodeOffset) const {
    KU_ASSERT(nodeOffset < offset->getNumValues());
    return offset->getData().getValue<offset_t>(nodeOffset);
}

offset_t ChunkedCSRHeader::getEndCSROffset(offset_t nodeOffset) const {
    KU_ASSERT(nodeOffset < offset->getNumValues());
    KU_ASSERT(nodeOffset < length->getNumValues());
    return getStartCSROffset(nodeOffset) + getCSRLength(nodeOffset);
}

length_t ChunkedCSRHeader::getCSRLength(offset_t nodeOffset) const {
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
        0 /*startNodeOffset*/, 0 /*startRowIdx*/);
    bool hasVersions;
    deSer.deserializeValue<bool>(hasVersions);
    if (hasVersions) {
        chunkedGroup->versionInfo = VersionInfo::deserialize(deSer);
    }
    return chunkedGroup;
}

} // namespace storage
} // namespace kuzu
