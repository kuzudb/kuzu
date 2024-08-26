#include "storage/store/chunked_node_group.h"

#include "common/assert.h"
#include "common/constants.h"
#include "common/types/types.h"
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

ChunkedNodeGroup::ChunkedNodeGroup(ChunkedNodeGroup& base,
    const std::vector<column_id_t>& selectedColumns)
    : format{base.format}, residencyState{base.residencyState}, startRowIdx{base.startRowIdx},
      capacity{base.capacity}, numRows{base.numRows.load()} {
    chunks.reserve(selectedColumns.size());
    for (const auto columnID : selectedColumns) {
        KU_ASSERT(columnID < base.getNumColumns());
        chunks.push_back(base.moveColumnChunk(columnID));
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

void ChunkedNodeGroup::resetToAllNull() const {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    for (const auto& chunk : chunks) {
        chunk->resetToAllNull();
    }
}

void ChunkedNodeGroup::resetNumRowsFromChunks() {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    KU_ASSERT(!chunks.empty());
    numRows = getColumnChunk(0).getNumValues();
    capacity = numRows;
    for (auto i = 1u; i < getNumColumns(); i++) {
        KU_ASSERT(numRows == getColumnChunk(i).getNumValues());
    }
}

void ChunkedNodeGroup::resizeChunks(const uint64_t newSize) {
    if (newSize <= capacity) {
        return;
    }
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    for (auto& chunk : chunks) {
        chunk->resize(newSize);
    }
    capacity = newSize;
}

void ChunkedNodeGroup::resetVersionAndUpdateInfo() {
    if (versionInfo) {
        versionInfo.reset();
    }
    for (const auto& chunk : chunks) {
        chunk->resetUpdateInfo();
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
    const auto numRowsToAppendInChunk = std::min(numValuesToAppend, capacity - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        const auto chunk = chunks[i].get();
        KU_ASSERT(i < columnVectors.size());
        const auto columnVector = columnVectors[i];
        // TODO(Guodong): Should add `slice` interface to SelVector.
        SelectionVector selVector(numRowsToAppendInChunk);
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

void ChunkedNodeGroup::write(const ChunkedNodeGroup& data, column_id_t offsetColumnID) {
    KU_ASSERT(residencyState == ResidencyState::IN_MEMORY);
    KU_ASSERT(data.chunks.size() == chunks.size() + 1);
    auto& offsetChunk = data.chunks[offsetColumnID];
    column_id_t columnID = 0, chunkIdx = 0;
    for (auto i = 0u; i < data.chunks.size(); i++) {
        if (i == offsetColumnID) {
            columnID++;
            continue;
        }
        KU_ASSERT(columnID < data.chunks.size());
        writeToColumnChunk(chunkIdx, columnID, data.chunks, *offsetChunk);
        chunkIdx++;
        columnID++;
    }
    numRows = chunks[0]->getNumValues();
    for (auto i = 1u; i < chunks.size(); i++) {
        KU_ASSERT(numRows == chunks[i]->getNumValues());
    }
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
void ChunkedNodeGroup::scanCommitted(Transaction* transaction, TableScanState& scanState,
    NodeGroupScanState& nodeGroupScanState, ChunkedNodeGroup& output) const {
    if (residencyState != SCAN_RESIDENCY_STATE) {
        return;
    }
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        const auto columnID = scanState.columnIDs[i];
        chunks[columnID]->scanCommitted<SCAN_RESIDENCY_STATE>(transaction,
            nodeGroupScanState.chunkStates[i], output.getColumnChunk(i));
    }
}

template void ChunkedNodeGroup::scanCommitted<ResidencyState::ON_DISK>(Transaction* transaction,
    TableScanState& scanState, NodeGroupScanState& nodeGroupScanState,
    ChunkedNodeGroup& output) const;
template void ChunkedNodeGroup::scanCommitted<ResidencyState::IN_MEMORY>(Transaction* transaction,
    TableScanState& scanState, NodeGroupScanState& nodeGroupScanState,
    ChunkedNodeGroup& output) const;

row_idx_t ChunkedNodeGroup::getNumDeletedRows(const Transaction* transaction) const {
    return versionInfo ? versionInfo->getNumDeletions(transaction) : 0;
}

row_idx_t ChunkedNodeGroup::getNumUpdatedRows(const Transaction* transaction,
    column_id_t columnID) {
    return getColumnChunk(columnID).getNumUpdatedRows(transaction);
}

std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>> ChunkedNodeGroup::scanUpdates(
    Transaction* transaction, column_id_t columnID) {
    return getColumnChunk(columnID).scanUpdates(transaction);
}

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
            if (columnID == INVALID_COLUMN_ID) {
                state.outputVectors[i]->setAllNull();
                continue;
            }
            if (columnID == ROW_IDX_COLUMN_ID) {
                state.rowIdxVector->setValue<row_idx_t>(
                    state.rowIdxVector->state->getSelVector()[posInOutput],
                    rowIdxInChunk + startRowIdx);
                continue;
            }
            KU_ASSERT(columnID < chunks.size());
            chunks[columnID]->lookup(transaction, nodeGroupScanState.chunkStates[i], rowIdxInChunk,
                *state.outputVectors[i],
                state.outputVectors[i]->state->getSelVector()[posInOutput]);
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

void ChunkedNodeGroup::addColumn(Transaction*, const TableAddColumnState& addColumnState,
    bool enableCompression, FileHandle* dataFH) {
    auto numRows = getNumRows();
    auto& dataType = addColumnState.propertyDefinition.getType();
    chunks.push_back(std::make_unique<ColumnChunk>(dataType, capacity, enableCompression,
        ResidencyState::IN_MEMORY));
    auto& chunkData = chunks.back()->getData();
    chunkData.populateWithDefaultVal(addColumnState.defaultEvaluator, numRows);
    if (residencyState == ResidencyState::ON_DISK) {
        KU_ASSERT(dataFH);
        chunkData.flush(*dataFH);
    }
}

bool ChunkedNodeGroup::isDeleted(const Transaction* transaction, row_idx_t rowInChunk) const {
    if (!versionInfo) {
        return false;
    }
    return versionInfo->isDeleted(transaction, rowInChunk);
}

bool ChunkedNodeGroup::isInserted(const Transaction* transaction, row_idx_t rowInChunk) const {
    if (!versionInfo) {
        return rowInChunk < getNumRows();
    }
    return versionInfo->isInserted(transaction, rowInChunk);
}

bool ChunkedNodeGroup::hasAnyUpdates(const Transaction* transaction, column_id_t columnID,
    row_idx_t startRow, length_t numRows) const {
    return getColumnChunk(columnID).hasUpdates(transaction, startRow, numRows);
}

row_idx_t ChunkedNodeGroup::getNumDeletions(const Transaction* transaction, row_idx_t startRow,
    length_t numRows) const {
    if (versionInfo) {
        return versionInfo->getNumDeletions(transaction, startRow, numRows);
    }
    return 0;
}

void ChunkedNodeGroup::finalize() const {
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->getData().finalize();
    }
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::flushAsNewChunkedNodeGroup(
    Transaction* transaction, FileHandle& dataFH) const {
    std::vector<std::unique_ptr<ColumnChunk>> flushedChunks(getNumColumns());
    for (auto i = 0u; i < getNumColumns(); i++) {
        flushedChunks[i] = std::make_unique<ColumnChunk>(getColumnChunk(i).isCompressionEnabled(),
            Column::flushChunkData(getColumnChunk(i).getData(), dataFH));
    }
    auto flushedChunkedGroup =
        std::make_unique<ChunkedNodeGroup>(std::move(flushedChunks), 0 /*startRowIdx*/);
    flushedChunkedGroup->versionInfo = std::make_unique<VersionInfo>();
    KU_ASSERT(flushedChunkedGroup->getNumRows() == numRows);
    flushedChunkedGroup->versionInfo->append(transaction, 0, numRows);
    return flushedChunkedGroup;
}

void ChunkedNodeGroup::flush(FileHandle& dataFH) {
    for (auto i = 0u; i < getNumColumns(); i++) {
        getColumnChunk(i).getData().flush(dataFH);
    }
    // Reset residencyState and numRows after flushing.
    residencyState = ResidencyState::ON_DISK;
    resetNumRowsFromChunks();
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
    serializer.writeDebuggingInfo("chunks");
    serializer.serializeVectorOfPtrs(chunks);
    serializer.writeDebuggingInfo("has_version_info");
    serializer.write<bool>(versionInfo != nullptr);
    if (versionInfo) {
        serializer.writeDebuggingInfo("version_info");
        versionInfo->serialize(serializer);
    }
}

std::unique_ptr<ChunkedNodeGroup> ChunkedNodeGroup::deserialize(Deserializer& deSer) {
    std::string key;
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    bool hasVersions;
    deSer.validateDebuggingInfo(key, "chunks");
    deSer.deserializeVectorOfPtrs<ColumnChunk>(chunks);
    auto chunkedGroup = std::make_unique<ChunkedNodeGroup>(std::move(chunks), 0 /*startRowIdx*/);
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
