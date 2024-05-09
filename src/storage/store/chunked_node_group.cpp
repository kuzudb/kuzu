#include "storage/store/chunked_node_group.h"

#include "common/assert.h"
#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool ChunkedCSRHeader::sanityCheck() const {
    if (offset->getNumValues() != length->getNumValues()) {
        return false;
    }
    if (offset->getNumValues() == 0) {
        return true;
    }
    if (offset->getValue<offset_t>(0) < length->getValue<length_t>(0)) {
        return false;
    }
    for (auto i = 1u; i < offset->getNumValues(); i++) {
        if (offset->getValue<offset_t>(i - 1) + length->getValue<length_t>(i) >
            offset->getValue<offset_t>(i)) {
            return false;
        }
    }
    return true;
}

void ChunkedCSRHeader::copyFrom(const ChunkedCSRHeader& other) const {
    auto numValues = other.offset->getNumValues();
    memcpy(offset->getData(), other.offset->getData(), numValues * sizeof(offset_t));
    memcpy(length->getData(), other.length->getData(), numValues * sizeof(length_t));
    length->setNumValues(numValues);
    offset->setNumValues(numValues);
}

void ChunkedCSRHeader::fillDefaultValues(offset_t newNumValues) const {
    auto lastCSROffset = getEndCSROffset(length->getNumValues() - 1);
    for (auto i = length->getNumValues(); i < newNumValues; i++) {
        offset->setValue<offset_t>(lastCSROffset, i);
        length->setValue<length_t>(0, i);
    }
    KU_ASSERT(
        offset->getNumValues() >= newNumValues && length->getNumValues() == offset->getNumValues());
}

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<common::LogicalType>& columnTypes,
    bool enableCompression, uint64_t capacity)
    : nodeGroupIdx{UINT64_MAX}, numRows{0} {
    chunks.reserve(columnTypes.size());
    for (auto& type : columnTypes) {
        chunks.push_back(
            ColumnChunkFactory::createColumnChunk(*type.copy(), enableCompression, capacity));
    }
}

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns,
    bool enableCompression)
    : nodeGroupIdx{UINT64_MAX}, numRows{0} {
    chunks.reserve(columns.size());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        chunks.push_back(ColumnChunkFactory::createColumnChunk(
            *columns[columnID]->getDataType().copy(), enableCompression));
    }
}

void ChunkedNodeGroup::resetToEmpty() {
    numRows = 0;
    nodeGroupIdx = UINT64_MAX;
    for (auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
}

void ChunkedNodeGroup::setAllNull() {
    for (auto& chunk : chunks) {
        chunk->getNullChunk()->resetToAllNull();
    }
}

void ChunkedNodeGroup::setNumRows(common::offset_t numRows_) {
    for (auto& chunk : chunks) {
        chunk->setNumValues(numRows_);
    }
    numRows = numRows_;
}

void ChunkedNodeGroup::resizeChunks(uint64_t newSize) {
    for (auto& chunk : chunks) {
        chunk->resize(newSize);
    }
}

uint64_t ChunkedNodeGroup::append(const std::vector<ValueVector*>& columnVectors,
    SelectionVector& selVector, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numRows);
    auto serialSkip = 0u;
    //    auto slicedSelVector = selVector.slice(numValuesToAppendInChunk);
    auto originalSize = selVector.getSelSize();
    selVector.setSelSize(numValuesToAppendInChunk);
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        if (chunk->getDataType().getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            chunk->setNumValues(chunk->getNumValues() + numValuesToAppendInChunk);
            serialSkip++;
            continue;
        }
        KU_ASSERT((i - serialSkip) < columnVectors.size());
        auto columnVector = columnVectors[i - serialSkip];
        chunk->append(columnVector, selVector);
    }
    selVector.setSelSize(originalSize);
    //    selVector.selectedSize = originalSize;
    numRows += numValuesToAppendInChunk;
    return numValuesToAppendInChunk;
}

offset_t ChunkedNodeGroup::append(ChunkedNodeGroup* other, offset_t offsetInOtherNodeGroup,
    offset_t numValues) {
    KU_ASSERT(other->chunks.size() == chunks.size());
    auto numNodesToAppend = std::min(std::min(numValues, other->numRows - offsetInOtherNodeGroup),
        chunks[0]->getCapacity() - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->append(other->chunks[i].get(), offsetInOtherNodeGroup, numNodesToAppend);
    }
    numRows += numNodesToAppend;
    return numNodesToAppend;
}

void ChunkedNodeGroup::write(const std::vector<std::unique_ptr<ColumnChunk>>& data,
    column_id_t offsetColumnID) {
    KU_ASSERT(data.size() == chunks.size() + 1);
    auto& offsetChunk = data[offsetColumnID];
    column_id_t columnID = 0, chunkIdx = 0;
    for (auto i = 0u; i < data.size(); i++) {
        if (i == offsetColumnID) {
            columnID++;
            continue;
        }
        KU_ASSERT(columnID < data.size());
        writeToColumnChunk(chunkIdx, columnID, data, *offsetChunk);
        chunkIdx++;
        columnID++;
    }
    numRows += offsetChunk->getNumValues();
}

void ChunkedNodeGroup::write(const ChunkedNodeGroup& data, column_id_t offsetColumnID) {
    write(data.chunks, offsetColumnID);
}

void ChunkedNodeGroup::finalize(uint64_t nodeGroupIdx_) {
    nodeGroupIdx = nodeGroupIdx_;
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->finalize();
    }
}

ChunkedCSRHeader::ChunkedCSRHeader(bool enableCompression, uint64_t capacity) {
    offset =
        ColumnChunkFactory::createColumnChunk(*LogicalType::UINT64(), enableCompression, capacity);
    length =
        ColumnChunkFactory::createColumnChunk(*LogicalType::UINT64(), enableCompression, capacity);
}

offset_t ChunkedCSRHeader::getStartCSROffset(offset_t nodeOffset) const {
    if (nodeOffset == 0 || offset->getNumValues() == 0) {
        return 0;
    }
    return offset->getValue<offset_t>(
        nodeOffset >= offset->getNumValues() ? (offset->getNumValues() - 1) : (nodeOffset - 1));
}

offset_t ChunkedCSRHeader::getEndCSROffset(offset_t nodeOffset) const {
    if (offset->getNumValues() == 0) {
        return 0;
    }
    return offset->getValue<offset_t>(
        nodeOffset >= offset->getNumValues() ? (offset->getNumValues() - 1) : nodeOffset);
}

length_t ChunkedCSRHeader::getCSRLength(offset_t nodeOffset) const {
    return nodeOffset >= length->getNumValues() ? 0 : length->getValue<length_t>(nodeOffset);
}

ChunkedCSRNodeGroup::ChunkedCSRNodeGroup(const std::vector<LogicalType>& columnTypes,
    bool enableCompression)
    // By default, initialize all column chunks except for the csrOffsetChunk to empty, as they
    // should be resized after csr offset calculation (e.g., during RelBatchInsert).
    : ChunkedNodeGroup{columnTypes, enableCompression, 0 /* capacity */} {
    csrHeader = ChunkedCSRHeader(enableCompression);
}

} // namespace storage
} // namespace kuzu
