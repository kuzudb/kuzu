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

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<LogicalType>& columnTypes,
    bool enableCompression, uint64_t capacity, const offset_t startOffset,
    ResidencyState residencyState)
    : residencyState{residencyState}, startNodeOffset{startOffset}, startRowIdx{INVALID_ROW_IDX},
      capacity{capacity}, numRows{0} {
    chunks.reserve(columnTypes.size());
    for (auto& dataType : columnTypes) {
        chunks.push_back(std::make_unique<ColumnChunk>(*dataType.copy(), capacity,
            enableCompression, residencyState));
    }
}

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns,
    bool enableCompression)
    : residencyState{ResidencyState::ON_DISK}, startNodeOffset{INVALID_OFFSET},
      startRowIdx{INVALID_ROW_IDX}, capacity{StorageConstants::NODE_GROUP_SIZE}, numRows{0} {
    chunks.reserve(columns.size());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        chunks.push_back(std::make_unique<ColumnChunk>(columns[columnID]->getDataType(), capacity,
            enableCompression, residencyState));
    }
}

void ChunkedNodeGroup::resetToEmpty() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    numRows = 0;
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

uint64_t ChunkedNodeGroup::append(const Transaction* transaction,
    const std::vector<ValueVector*>& columnVectors, SelectionVector& selVector,
    uint64_t numValuesToAppend) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    const auto numRowsToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numRows);
    const auto originalSize = selVector.getSelSize();
    selVector.setSelSize(numRowsToAppendInChunk);
    for (auto i = 0u; i < chunks.size(); i++) {
        const auto chunk = chunks[i].get();
        KU_ASSERT(i < columnVectors.size());
        const auto columnVector = columnVectors[i];
        chunk->getData().append(columnVector, selVector);
    }
    selVector.setSelSize(originalSize);
    if (!versionInfo) {
        versionInfo = std::make_unique<VersionInfo>();
    }
    versionInfo->append(transaction, numRows - startRowIdx, numRowsToAppendInChunk);
    numRows += numRowsToAppendInChunk;
    return numRowsToAppendInChunk;
}

offset_t ChunkedNodeGroup::append(const Transaction* transaction, const ChunkedNodeGroup& other,
    offset_t offsetInOtherNodeGroup, offset_t numValues) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    KU_ASSERT(other.chunks.size() == chunks.size());
    const auto numNodesToAppend =
        std::min(std::min(numValues, other.numRows - offsetInOtherNodeGroup),
            chunks[0]->getData().getCapacity() - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().append(&other.chunks[i]->getData(), offsetInOtherNodeGroup,
            numNodesToAppend);
    }
    if (!versionInfo) {
        versionInfo = std::make_unique<VersionInfo>();
    }
    versionInfo->append(transaction, numRows - startRowIdx, numNodesToAppend);
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
    NodeGroupScanState& nodeGroupScanState, offset_t offsetInGroup, length_t length) const {
    KU_ASSERT(offsetInGroup + length <= numRows);
    // TODO: Combine version info.
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        const auto columnID = scanState.columnIDs[i];
        KU_ASSERT(columnID < chunks.size());
        chunks[columnID]->scan(transaction, nodeGroupScanState.chunkStates[i], *scanState.IDVector,
            *scanState.outputVectors[i], offsetInGroup, length);
    }
}

template<ResidencyState SCAN_RESIDENCY_STATE>
void ChunkedNodeGroup::scanCommitted(Transaction* transaction, TableScanState& scanState,
    NodeGroupScanState& nodeGroupScanState, ChunkedNodeGroup& output) const {
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->scanCommitted<SCAN_RESIDENCY_STATE>(transaction,
            nodeGroupScanState.chunkStates[i], output.getColumnChunk(i));
    }
}

template void ChunkedNodeGroup::scanCommitted<ResidencyState::ON_DISK>(Transaction* transaction,
    TableScanState& scanState, NodeGroupScanState& nodeGroupScanState,
    ChunkedNodeGroup& output) const;
template void ChunkedNodeGroup::scanCommitted<ResidencyState::IN_MEMORY>(Transaction* transaction,
    TableScanState& scanState, NodeGroupScanState& nodeGroupScanState,
    ChunkedNodeGroup& output) const;

void ChunkedNodeGroup::lookup(Transaction* transaction, TableScanState& state,
    NodeGroupScanState& nodeGroupScanState, offset_t offsetInGroup) const {
    KU_ASSERT(offsetInGroup + 1 <= numRows);
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        const auto columnID = state.columnIDs[i];
        KU_ASSERT(columnID < chunks.size());
        KU_ASSERT(state.outputVectors[i]->state->getSelVector().getSelSize() == 1);
        chunks[columnID]->lookup(transaction, nodeGroupScanState.chunkStates[i], offsetInGroup,
            *state.outputVectors[i],
            state.outputVectors[i]->state->getSelVector().getSelectedPositions()[0]);
    }
}

void ChunkedNodeGroup::update(Transaction* transaction, offset_t offset, column_id_t columnID,
    ValueVector& propertyVector) {
    getColumnChunk(columnID).update(transaction, offset - startNodeOffset, propertyVector);
}

bool ChunkedNodeGroup::delete_(Transaction* transaction, offset_t offset) {
    if (!versionInfo) {
        versionInfo = std::make_unique<VersionInfo>();
    }
    return versionInfo->delete_(transaction, offset - startRowIdx);
}

void ChunkedNodeGroup::finalize() const {
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
    return std::make_unique<ChunkedNodeGroup>(std::move(flushedChunks), 0 /*startNodeOffset*/,
        0 /*startRowIdx*/);
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

bool ChunkedNodeGroup::hasUpdates() const {
    for (const auto& chunk : chunks) {
        if (chunk->hasUpdates()) {
            return true;
        }
    }
    return false;
}

void ChunkedNodeGroup::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    serializer.serializeVectorOfPtrs(chunks);
    serializer.write<bool>(versionInfo != nullptr);
    if (versionInfo) {
        versionInfo->serialize(serializer);
    }
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::deserialize(Deserializer& deSer) {
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks);
    auto chunkedGroup = std::make_unique<ChunkedNodeGroup>(std::move(chunks), 0 /*startNodeOffset*/,
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
