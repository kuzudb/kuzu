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

ChunkedNodeGroup::ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks,
    row_idx_t startRowIdx, NodeGroupDataFormat format)
    : format{format}, startRowIdx{startRowIdx}, chunks{std::move(chunks)} {
    KU_ASSERT(!this->chunks.empty());
    residencyState = this->chunks[0]->getResidencyState();
    numRows = this->chunks[0]->getNumValues();
    capacity = numRows;
    for (auto columnID = 1u; columnID < this->chunks.size(); columnID++) {
        KU_ASSERT(this->chunks[columnID]->getNumValues() == numRows);
        KU_ASSERT(this->chunks[columnID]->getResidencyState() == residencyState);
    }
}

ChunkedNodeGroup::ChunkedNodeGroup(const std::vector<LogicalType>& columnTypes,
    bool enableCompression, uint64_t capacity, row_idx_t startRowIdx, ResidencyState residencyState,
    NodeGroupDataFormat format)
    : format{format}, residencyState{residencyState}, startRowIdx{startRowIdx}, capacity{capacity},
      numRows{0} {
    chunks.reserve(columnTypes.size());
    for (auto& type : columnTypes) {
        chunks.push_back(std::make_unique<ColumnChunk>(type.copy(), capacity, enableCompression,
            residencyState));
    }
}

void ChunkedNodeGroup::resetToEmpty() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    numRows = 0;
    for (const auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
    versionInfo.reset();
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
    const std::vector<ValueVector*>& columnVectors, row_idx_t startRowInVectors,
    uint64_t numValuesToAppend) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    const auto numRowsToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        const auto chunk = chunks[i].get();
        KU_ASSERT(i < columnVectors.size());
        const auto columnVector = columnVectors[i];
        // TODO(Guodong): Should add `slice` interface to SelVector.
        SelectionVector selVector;
        for (auto row = 0u; row < numRowsToAppendInChunk; row++) {
            selVector.getMultableBuffer()[row] =
                columnVector->state->getSelVector()[startRowInVectors + row];
        }
        selVector.setToFiltered(numRowsToAppendInChunk);
        chunk->getData().append(columnVector, selVector);
    }
    if (transaction->getID() != Transaction::DUMMY_TRANSACTION_ID) {
        if (!versionInfo) {
            versionInfo = std::make_unique<VersionInfo>();
        }
        versionInfo->append(transaction, numRows, numRowsToAppendInChunk);
    }
    numRows += numRowsToAppendInChunk;
    return numRowsToAppendInChunk;
}

offset_t ChunkedNodeGroup::append(const Transaction* transaction, const ChunkedNodeGroup& other,
    offset_t offsetInOtherNodeGroup, offset_t numRowsToAppend) {
    KU_ASSERT(residencyState == ResidencyState::IN_MEMORY);
    KU_ASSERT(other.chunks.size() == chunks.size());
    std::vector<ColumnChunk*> chunksToAppend(other.chunks.size());
    for (auto i = 0u; i < chunks.size(); i++) {
        chunksToAppend[i] = other.chunks[i].get();
    }
    return append(transaction, chunksToAppend, offsetInOtherNodeGroup, numRowsToAppend);
}

offset_t ChunkedNodeGroup::append(const Transaction* transaction,
    const std::vector<ColumnChunk*>& other, offset_t offsetInOtherNodeGroup,
    offset_t numRowsToAppend) {
    KU_ASSERT(residencyState == ResidencyState::IN_MEMORY);
    KU_ASSERT(other.size() == chunks.size());
    const auto numToAppendInChunkedGroup = std::min(numRowsToAppend, capacity - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().append(&other[i]->getData(), offsetInOtherNodeGroup,
            numToAppendInChunkedGroup);
    }
    if (transaction->getID() != Transaction::DUMMY_TRANSACTION_ID) {
        if (!versionInfo) {
            versionInfo = std::make_unique<VersionInfo>();
        }
        versionInfo->append(transaction, numRows, numToAppendInChunkedGroup);
    }
    numRows += numToAppendInChunkedGroup;
    return numToAppendInChunkedGroup;
}

void ChunkedNodeGroup::write(const std::vector<std::unique_ptr<ColumnChunk>>& data,
    column_id_t offsetColumnID) {
    KU_ASSERT(residencyState == ResidencyState::IN_MEMORY);
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

void ChunkedNodeGroup::scan(const Transaction* transaction, const TableScanState& scanState,
    const NodeGroupScanState& nodeGroupScanState, offset_t rowIdxInGroup,
    length_t numRowsToScan) const {
    KU_ASSERT(rowIdxInGroup + numRowsToScan <= numRows);
    bool hasValuesToScan = true;
    std::unique_ptr<SelectionVector> selVector = nullptr;
    if (versionInfo) {
        selVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        versionInfo->getSelVectorToScan(transaction->getStartTS(), transaction->getID(), *selVector,
            rowIdxInGroup, numRowsToScan);
        hasValuesToScan = selVector->getSelSize() > 0;
    }
    auto& anchorSelVector = scanState.IDVector->state->getSelVectorUnsafe();
    if (selVector && selVector->getSelSize() != numRowsToScan) {
        std::memcpy(anchorSelVector.getMultableBuffer().data(),
            selVector->getMultableBuffer().data(), selVector->getSelSize() * sizeof(sel_t));
        anchorSelVector.setToFiltered(selVector->getSelSize());
    } else {
        anchorSelVector.setToUnfiltered(numRowsToScan);
    }
    if (hasValuesToScan) {
        for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
            const auto columnID = scanState.columnIDs[i];
            if (columnID == INVALID_COLUMN_ID) {
                scanState.outputVectors[i]->setAllNull();
                continue;
            }
            if (columnID == ROW_IDX_COLUMN_ID) {
                for (auto rowIdx = 0u; rowIdx < numRowsToScan; rowIdx++) {
                    scanState.rowIdxVector->setValue<row_idx_t>(rowIdx,
                        rowIdx + rowIdxInGroup + startRowIdx);
                }
                continue;
            }
            KU_ASSERT(columnID < chunks.size());
            chunks[columnID]->scan(transaction, nodeGroupScanState.chunkStates[i],
                *scanState.IDVector, *scanState.outputVectors[i], rowIdxInGroup, numRowsToScan);
        }
    }
}

template<ResidencyState SCAN_RESIDENCY_STATE>
void ChunkedNodeGroup::scanCommitted(Transaction* transaction, TableScanState&,
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

bool ChunkedNodeGroup::lookup(Transaction* transaction, const TableScanState& state,
    NodeGroupScanState& nodeGroupScanState, offset_t rowIdxInChunk, sel_t posInOutput) const {
    KU_ASSERT(rowIdxInChunk + 1 <= numRows);
    std::unique_ptr<SelectionVector> selVector = nullptr;
    bool hasValuesToScan = true;
    if (versionInfo) {
        selVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        versionInfo->getSelVectorToScan(transaction->getStartTS(), transaction->getID(), *selVector,
            rowIdxInChunk, 1);
        hasValuesToScan = selVector->getSelSize() > 0;
    }
    if (hasValuesToScan) {
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            const auto columnID = state.columnIDs[i];
            KU_ASSERT(columnID < chunks.size());
            chunks[columnID]->lookup(transaction, nodeGroupScanState.chunkStates[i], rowIdxInChunk,
                *state.outputVectors[i],
                state.outputVectors[i]->state->getSelVector().getSelectedPositions()[posInOutput]);
        }
    }
    return hasValuesToScan;
}

void ChunkedNodeGroup::update(Transaction* transaction, row_idx_t rowIdxInChunk,
    column_id_t columnID, const ValueVector& propertyVector) {
    getColumnChunk(columnID).update(transaction, rowIdxInChunk, propertyVector);
}

bool ChunkedNodeGroup::delete_(const Transaction* transaction, row_idx_t rowIdxInChunk) {
    if (!versionInfo) {
        versionInfo = std::make_unique<VersionInfo>();
    }
    return versionInfo->delete_(transaction, rowIdxInChunk);
}

void ChunkedNodeGroup::addColumn(Transaction* transaction, TableAddColumnState& addColumnState,
    bool enableCompression) {
    auto numRows = getNumRows();
    auto chunkData = 
        ColumnChunkFactory::createColumnChunkData(addColumnState.property.getDataType().copy(),
            enableCompression, capacity, ResidencyState::IN_MEMORY);
    chunkData->populateWithDefaultVal(addColumnState.defaultEvaluator, numRows);
    chunks.push_back(std::make_unique<ColumnChunk>(enableCompression, std::move(chunkData)));
}

void ChunkedNodeGroup::finalize() const {
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().finalize();
    }
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::flushAsNewChunkedNodeGroup(
    BMFileHandle& dataFH) const {
    std::vector<std::unique_ptr<ColumnChunk>> flushedChunks(getNumColumns());
    for (auto i = 0u; i < getNumColumns(); i++) {
        flushedChunks[i] = std::make_unique<ColumnChunk>(getColumnChunk(i).isCompressionEnabled(),
            Column::flushChunkData(getColumnChunk(i).getData(), dataFH));
    }
    return std::make_unique<ChunkedNodeGroup>(std::move(flushedChunks), 0 /*startRowIdx*/);
}

void ChunkedNodeGroup::flush(BMFileHandle& dataFH) {
    for (auto i = 0u; i < getNumColumns(); i++) {
        getColumnChunk(i).getData().flush(dataFH);
    }
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
    auto chunkedGroup = std::make_unique<ChunkedNodeGroup>(std::move(chunks), 0 /*startRowIdx*/);
    bool hasVersions;
    deSer.deserializeValue<bool>(hasVersions);
    if (hasVersions) {
        chunkedGroup->versionInfo = VersionInfo::deserialize(deSer);
    }
    return chunkedGroup;
}

} // namespace storage
} // namespace kuzu
