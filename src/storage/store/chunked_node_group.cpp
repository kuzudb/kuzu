#include "storage/store/chunked_node_group.h"

#include "common/assert.h"
#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {
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

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<LogicalType>& columnTypes,
    bool enableCompression, uint64_t capacity, const offset_t startOffset,
    ResidencyState residencyState)
    : residencyState{residencyState}, nodeGroupIdx{INVALID_NODE_GROUP_IDX},
      startNodeOffset{startOffset}, capacity{capacity}, numRows{0} {
    chunks.reserve(columnTypes.size());
    for (auto& dataType : columnTypes) {
        chunks.push_back(std::make_unique<ColumnChunk>(*dataType.copy(), capacity,
            enableCompression, residencyState));
    }
}

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns,
    bool enableCompression)
    : residencyState{ResidencyState::ON_DISK}, nodeGroupIdx{INVALID_NODE_GROUP_IDX},
      startNodeOffset{INVALID_OFFSET}, capacity{StorageConstants::NODE_GROUP_SIZE}, numRows{0} {
    chunks.reserve(columns.size());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        chunks.push_back(std::make_unique<ColumnChunk>(columns[columnID]->getDataType(), capacity,
            enableCompression, residencyState));
    }
}

void ChunkedNodeGroup::resetToEmpty() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    numRows = 0;
    nodeGroupIdx = UINT64_MAX;
    for (const auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
}

void ChunkedNodeGroup::setAllNull() const {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    for (const auto& chunk : chunks) {
        chunk->setAllNull();
    }
}

void ChunkedNodeGroup::resizeChunks(const uint64_t newSize) const {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    for (auto& chunk : chunks) {
        chunk->resize(newSize);
    }
}

void ChunkedNodeGroup::setNumRows(const offset_t numRows_) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    for (const auto& chunk : chunks) {
        chunk->setNumValues(numRows_);
    }
    numRows = numRows_;
}

uint64_t ChunkedNodeGroup::append(const std::vector<ValueVector*>& columnVectors,
    SelectionVector& selVector, uint64_t numValuesToAppend) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numRows);
    auto originalSize = selVector.getSelSize();
    selVector.setSelSize(numValuesToAppendInChunk);
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        KU_ASSERT(i < columnVectors.size());
        auto columnVector = columnVectors[i];
        chunk->getData().append(columnVector, selVector);
    }
    selVector.setSelSize(originalSize);
    numRows += numValuesToAppendInChunk;
    return numValuesToAppendInChunk;
}

offset_t ChunkedNodeGroup::append(const ChunkedNodeGroup& other, offset_t offsetInOtherNodeGroup,
    offset_t numValues) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    KU_ASSERT(other.chunks.size() == chunks.size());
    const auto numNodesToAppend =
        std::min(std::min(numValues, other.numRows - offsetInOtherNodeGroup),
            chunks[0]->getData().getCapacity() - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().append(&other.chunks[i]->getData(), offsetInOtherNodeGroup,
            numNodesToAppend);
    }
    numRows += numNodesToAppend;
    return numNodesToAppend;
}

void ChunkedNodeGroup::write(const std::vector<std::unique_ptr<ColumnChunk>>& data,
    column_id_t offsetColumnID) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
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

void ChunkedNodeGroup::scan(Transaction* transaction, TableScanState& scanState,
    offset_t offsetInGroup, length_t length) const {
    KU_ASSERT(offsetInGroup + length <= numRows);
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        const auto columnID = scanState.columnIDs[i];
        KU_ASSERT(columnID < chunks.size());
        chunks[columnID]->scan(transaction, scanState.cast<NodeTableScanState>().chunkStates[i],
            *scanState.nodeIDVector, *scanState.outputVectors[i], offsetInGroup, length);
    }
}

void ChunkedNodeGroup::lookup(Transaction* transaction, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors, offset_t offsetInGroup) const {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    KU_ASSERT(offsetInGroup + 1 <= numRows);
    for (auto i = 0u; i < columnIDs.size(); i++) {
        const auto columnID = columnIDs[i];
        KU_ASSERT(columnID < chunks.size());
        KU_ASSERT(outputVectors[i]->state->getSelVector().getSelSize() == 1);
        chunks[columnID]->lookup(transaction, offsetInGroup, *outputVectors[i],
            outputVectors[i]->state->getSelVector().getSelectedPositions()[0]);
    }
}

void ChunkedNodeGroup::update(Transaction* transaction, offset_t offset, column_id_t columnID,
    ValueVector& propertyVector) {
    getColumnChunk(columnID).update(transaction, offset - startNodeOffset, propertyVector);
}

void ChunkedNodeGroup::finalize(uint64_t nodeGroupIdx_) {
    nodeGroupIdx = nodeGroupIdx_;
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().finalize();
    }
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::flush(BMFileHandle& dataFH) const {
    std::vector<std::unique_ptr<ColumnChunk>> flushedChunks(getNumColumns());
    for (auto i = 0u; i < getNumColumns(); i++) {
        flushedChunks[i] = std::make_unique<ColumnChunk>(getColumnChunk(i).isCompressionEnabled(),
            Column::flushChunkData(getColumnChunk(i).getData(), dataFH));
    }
    return std::make_unique<ChunkedNodeGroup>(std::move(flushedChunks), 0 /*startNodeOffset*/);
}

uint64_t ChunkedNodeGroup::getEstimatedMemoryUsage() const {
    if (residencyState == ResidencyState::ON_DISK) {
        return 0;
    }
    uint64_t memoryUsage = 0;
    for (const auto& chunk : chunks) {
        memoryUsage += chunk->getEstimatedMemoryUsage();
    }
    return memoryUsage;
}

void ChunkedNodeGroup::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    serializer.serializeVectorOfPtrs(chunks);
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::deserialize(Deserializer& deSer) {
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks);
    return std::make_unique<ChunkedNodeGroup>(std::move(chunks), 0 /*startNodeOffset*/);
}

ChunkedCSRHeader::ChunkedCSRHeader(bool enableCompression, uint64_t capacity,
    ResidencyState residencyState) {
    offset = std::make_unique<ColumnChunk>(*LogicalType::UINT64(), enableCompression, capacity,
        residencyState);
    length = std::make_unique<ColumnChunk>(*LogicalType::UINT64(), enableCompression, capacity,
        residencyState);
}

offset_t ChunkedCSRHeader::getStartCSROffset(offset_t nodeOffset) const {
    KU_ASSERT(offset->getResidencyState() != ResidencyState::ON_DISK &&
              length->getResidencyState() != ResidencyState::ON_DISK);
    if (nodeOffset == 0 || offset->getNumValues() == 0) {
        return 0;
    }
    return offset->getData().getValue<offset_t>(
        nodeOffset >= offset->getNumValues() ? (offset->getNumValues() - 1) : (nodeOffset - 1));
}

offset_t ChunkedCSRHeader::getEndCSROffset(offset_t nodeOffset) const {
    KU_ASSERT(offset->getResidencyState() != ResidencyState::ON_DISK &&
              length->getResidencyState() != ResidencyState::ON_DISK);
    if (offset->getNumValues() == 0) {
        return 0;
    }
    return offset->getData().getValue<offset_t>(
        nodeOffset >= offset->getNumValues() ? (offset->getNumValues() - 1) : nodeOffset);
}

length_t ChunkedCSRHeader::getCSRLength(offset_t nodeOffset) const {
    KU_ASSERT(offset->getResidencyState() != ResidencyState::ON_DISK &&
              length->getResidencyState() != ResidencyState::ON_DISK);
    return nodeOffset >= length->getNumValues() ? 0 :
                                                  length->getData().getValue<length_t>(nodeOffset);
}

ChunkedCSRNodeGroup::ChunkedCSRNodeGroup(const std::vector<LogicalType>& columnTypes,
    bool enableCompression, uint64_t capacity, ResidencyState residencyState)
    // By default, initialize all column chunks except for the csrOffsetChunk to empty, as they
    // should be resized after csr offset calculation (e.g., during RelBatchInsert).
    : ChunkedNodeGroup{columnTypes, enableCompression, 0 /* capacity */, INVALID_OFFSET,
          residencyState} {
    csrHeader = ChunkedCSRHeader(enableCompression, capacity, residencyState);
}

} // namespace storage
} // namespace kuzu
