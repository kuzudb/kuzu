#include "storage/table/node_group.h"

#include "common/assert.h"
#include "common/types/types.h"
#include "common/uniq_lock.h"
#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/enums/residency_state.h"
#include "storage/storage_utils.h"
#include "storage/table/chunked_node_group.h"
#include "storage/table/column_chunk.h"
#include "storage/table/csr_chunked_node_group.h"
#include "storage/table/csr_node_group.h"
#include "storage/table/node_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::append(const Transaction* transaction,
    const std::vector<column_id_t>& columnIDs, ChunkedNodeGroup& chunkedGroup,
    row_idx_t startRowIdx, row_idx_t numRowsToAppend) {
    KU_ASSERT(numRowsToAppend <= chunkedGroup.getNumRows());
    std::vector<ColumnChunk*> chunksToAppend(chunkedGroup.getNumColumns());
    for (auto i = 0u; i < chunkedGroup.getNumColumns(); i++) {
        chunksToAppend[i] = &chunkedGroup.getColumnChunk(i);
    }
    return append(transaction, columnIDs, chunksToAppend, startRowIdx, numRowsToAppend);
}

row_idx_t NodeGroup::append(const Transaction* transaction,
    const std::vector<column_id_t>& columnIDs, const std::vector<ColumnChunk*>& chunkedGroup,
    row_idx_t startRowIdx, row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows();
    auto& mm = *transaction->getClientContext()->getMemoryManager();
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                StorageConfig::CHUNKED_NODE_GROUP_CAPACITY, 0, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        auto lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        if (!lastChunkedGroup || lastChunkedGroup->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                    StorageConfig::CHUNKED_NODE_GROUP_CAPACITY,
                    numRowsBeforeAppend + numRowsAppended, ResidencyState::IN_MEMORY));
        }
        lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        KU_ASSERT(StorageConfig::CHUNKED_NODE_GROUP_CAPACITY >= lastChunkedGroup->getNumRows());
        auto numToCopyIntoChunk =
            StorageConfig::CHUNKED_NODE_GROUP_CAPACITY - lastChunkedGroup->getNumRows();
        const auto numToAppendInChunk =
            std::min(numRowsToAppend - numRowsAppended, numToCopyIntoChunk);
        lastChunkedGroup->append(transaction, columnIDs, chunkedGroup,
            numRowsAppended + startRowIdx, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
    }
    numRows += numRowsAppended;
    return numRowsBeforeAppend;
}

void NodeGroup::append(const Transaction* transaction, const std::vector<ValueVector*>& vectors,
    const row_idx_t startRowIdx, const row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows();
    auto& mm = *transaction->getClientContext()->getMemoryManager();
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                StorageConfig::CHUNKED_NODE_GROUP_CAPACITY, 0 /*startOffset*/,
                ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0;
    while (numRowsAppended < numRowsToAppend) {
        auto lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        if (!lastChunkedGroup || lastChunkedGroup->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                    StorageConfig::CHUNKED_NODE_GROUP_CAPACITY,
                    numRowsBeforeAppend + numRowsAppended, ResidencyState::IN_MEMORY));
        }
        lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            StorageConfig::CHUNKED_NODE_GROUP_CAPACITY - lastChunkedGroup->getNumRows());
        lastChunkedGroup->append(&DUMMY_TRANSACTION, vectors, startRowIdx + numRowsAppended,
            numRowsToAppendInGroup);
        numRowsAppended += numRowsToAppendInGroup;
    }
    numRows += numRowsAppended;
}

void NodeGroup::merge(Transaction*, std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == dataTypes.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType().getPhysicalType() ==
                  dataTypes[i].getPhysicalType());
    }
    const auto lock = chunkedGroups.lock();
    numRows += chunkedGroup->getNumRows();
    chunkedGroups.appendGroup(lock, std::move(chunkedGroup));
}

void NodeGroup::initializeScanState(const Transaction* transaction, TableScanState& state) const {
    const auto lock = chunkedGroups.lock();
    initializeScanState(transaction, lock, state);
}

static void initializeScanStateForChunkedGroup(const TableScanState& state,
    const ChunkedNodeGroup* chunkedGroup) {
    KU_ASSERT(chunkedGroup);
    if (chunkedGroup->getResidencyState() != ResidencyState::ON_DISK) {
        return;
    }
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        KU_ASSERT(i < state.columnIDs.size());
        KU_ASSERT(i < nodeGroupScanState.chunkStates.size());
        const auto columnID = state.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID || columnID == ROW_IDX_COLUMN_ID) {
            continue;
        }
        auto& chunk = chunkedGroup->getColumnChunk(columnID);
        auto& chunkState = nodeGroupScanState.chunkStates[i];
        chunk.initializeScanState(chunkState, state.columns[i]);
    }
}

void NodeGroup::initializeScanState(const Transaction*, const UniqLock& lock,
    TableScanState& state) const {
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    nodeGroupScanState.chunkedGroupIdx = 0;
    ChunkedNodeGroup* firstChunkedGroup = chunkedGroups.getFirstGroup(lock);
    nodeGroupScanState.nextRowToScan = firstChunkedGroup->getStartRowIdx();
    initializeScanStateForChunkedGroup(state, firstChunkedGroup);
}

void applySemiMaskFilter(const TableScanState& state, row_idx_t numRowsToScan,
    SelectionVector& selVector) {
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    const auto startNodeOffset = nodeGroupScanState.nextRowToScan +
                                 StorageUtils::getStartOffsetOfNodeGroup(state.nodeGroupIdx);
    const auto endNodeOffset = startNodeOffset + numRowsToScan;
    const auto& arr = state.semiMask->range(startNodeOffset, endNodeOffset);
    if (arr.empty()) {
        selVector.setSelSize(0);
    } else {
        auto stat = selVector.getMutableBuffer();
        uint64_t numSelectedValues = 0;
        size_t i = 0, j = 0;
        while (i < numRowsToScan && j < arr.size()) {
            auto temp = arr[j] - startNodeOffset;
            if (selVector[i] < temp) {
                ++i;
            } else if (selVector[i] > temp) {
                ++j;
            } else {
                stat[numSelectedValues++] = temp;
                ++i;
                ++j;
            }
        }
        selVector.setToFiltered(numSelectedValues);
    }
}

NodeGroupScanResult NodeGroup::scan(const Transaction* transaction, TableScanState& state) const {
    // TODO(Guodong): Move the locked part of figuring out the chunked group to initScan.
    const auto lock = chunkedGroups.lock();
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    KU_ASSERT(nodeGroupScanState.chunkedGroupIdx < chunkedGroups.getNumGroups(lock));
    const auto chunkedGroup = chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
    if (nodeGroupScanState.nextRowToScan >=
        chunkedGroup->getNumRows() + chunkedGroup->getStartRowIdx()) {
        nodeGroupScanState.chunkedGroupIdx++;
        if (nodeGroupScanState.chunkedGroupIdx >= chunkedGroups.getNumGroups(lock)) {
            return NODE_GROUP_SCAN_EMMPTY_RESULT;
        }
        ChunkedNodeGroup* currentChunkedGroup =
            chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
        initializeScanStateForChunkedGroup(state, currentChunkedGroup);
    }
    const auto& chunkedGroupToScan =
        *chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
    KU_ASSERT(nodeGroupScanState.nextRowToScan >= chunkedGroupToScan.getStartRowIdx());
    const auto rowIdxInChunkToScan =
        nodeGroupScanState.nextRowToScan - chunkedGroupToScan.getStartRowIdx();
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - rowIdxInChunkToScan, DEFAULT_VECTOR_CAPACITY);
    bool enableSemiMask =
        state.source == TableScanSource::COMMITTED && state.semiMask && state.semiMask->isEnabled();
    if (enableSemiMask) {
        applySemiMaskFilter(state, numRowsToScan, state.outState->getSelVectorUnsafe());
        if (state.outState->getSelVector().getSelSize() == 0) {
            state.nodeGroupScanState->nextRowToScan += numRowsToScan;
            return NodeGroupScanResult{nodeGroupScanState.nextRowToScan, 0};
        }
    }
    chunkedGroupToScan.scan(transaction, state, nodeGroupScanState, rowIdxInChunkToScan,
        numRowsToScan);
    const auto startRow = nodeGroupScanState.nextRowToScan;
    nodeGroupScanState.nextRowToScan += numRowsToScan;
    return NodeGroupScanResult{startRow, numRowsToScan};
}

NodeGroupScanResult NodeGroup::scan(Transaction* transaction, TableScanState& state,
    offset_t startOffset, offset_t numRowsToScan) const {
    bool enableSemiMask =
        state.source == TableScanSource::COMMITTED && state.semiMask && state.semiMask->isEnabled();
    if (enableSemiMask) {
        applySemiMaskFilter(state, numRowsToScan, state.outState->getSelVectorUnsafe());
        if (state.outState->getSelVector().getSelSize() == 0) {
            state.nodeGroupScanState->nextRowToScan += numRowsToScan;
            return NodeGroupScanResult{state.nodeGroupScanState->nextRowToScan, 0};
        }
    }
    if (state.outputVectors.size() == 0) {
        auto startOffsetInGroup =
            startOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        KU_ASSERT(scanInternal(chunkedGroups.lock(), transaction, state, startOffset,
                      numRowsToScan) == NodeGroupScanResult(startOffsetInGroup, numRowsToScan));
        return NodeGroupScanResult{startOffsetInGroup, numRowsToScan};
    }
    return scanInternal(chunkedGroups.lock(), transaction, state, startOffset, numRowsToScan);
}

NodeGroupScanResult NodeGroup::scanInternal(const UniqLock& lock, Transaction* transaction,
    TableScanState& state, offset_t startOffset, offset_t numRowsToScan) const {
    // Only meant for scanning once
    KU_ASSERT(numRowsToScan <= DEFAULT_VECTOR_CAPACITY);

    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    KU_ASSERT(startOffset >= nodeGroupStartOffset);
    auto startOffsetInGroup = startOffset - nodeGroupStartOffset;
    KU_ASSERT(startOffsetInGroup + numRowsToScan <= numRows);

    auto& nodeGroupScanState = *state.nodeGroupScanState;
    nodeGroupScanState.nextRowToScan = startOffsetInGroup;

    auto [newChunkedGroupIdx, _] = findChunkedGroupIdxFromRowIdxNoLock(startOffsetInGroup);

    const auto* chunkedGroupToScan = chunkedGroups.getGroup(lock, newChunkedGroupIdx);
    if (newChunkedGroupIdx != nodeGroupScanState.chunkedGroupIdx) {
        // If the chunked group matches the scan state, don't re-initialize it.
        // E.g. we may scan a group multiple times in parts
        initializeScanStateForChunkedGroup(state, chunkedGroupToScan);
        nodeGroupScanState.chunkedGroupIdx = newChunkedGroupIdx;
    }

    uint64_t numRowsScanned = 0;
    do {
        const auto rowIdxInChunkToScan =
            (startOffsetInGroup + numRowsScanned) - chunkedGroupToScan->getStartRowIdx();

        uint64_t numRowsToScanInChunk = std::min(numRowsToScan - numRowsScanned,
            chunkedGroupToScan->getNumRows() - rowIdxInChunkToScan);
        chunkedGroupToScan->scan(transaction, state, nodeGroupScanState, rowIdxInChunkToScan,
            numRowsToScanInChunk);
        numRowsScanned += numRowsToScanInChunk;
        nodeGroupScanState.nextRowToScan += numRowsToScanInChunk;
        if (numRowsScanned < numRowsToScan) {
            nodeGroupScanState.chunkedGroupIdx++;
            chunkedGroupToScan = chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
        }
    } while (numRowsScanned < numRowsToScan);

    return NodeGroupScanResult{startOffsetInGroup, numRowsScanned};
}

bool NodeGroup::lookup(const UniqLock& lock, const Transaction* transaction,
    const TableScanState& state, sel_t posInSel) const {
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    const auto pos = state.rowIdxVector->state->getSelVector().getSelectedPositions()[posInSel];
    KU_ASSERT(!state.rowIdxVector->isNull(pos));
    const auto rowIdx = state.rowIdxVector->getValue<row_idx_t>(pos);
    const ChunkedNodeGroup* chunkedGroupToScan = findChunkedGroupFromRowIdx(lock, rowIdx);
    const auto rowIdxInChunkedGroup = rowIdx - chunkedGroupToScan->getStartRowIdx();
    return chunkedGroupToScan->lookup(transaction, state, nodeGroupScanState, rowIdxInChunkedGroup,
        posInSel);
}

bool NodeGroup::lookupMultiple(const UniqLock& lock, const Transaction* transaction,
    const TableScanState& state) const {
    idx_t numTuplesFound = 0;
    for (auto i = 0u; i < state.rowIdxVector->state->getSelVector().getSelSize(); i++) {
        auto& nodeGroupScanState = *state.nodeGroupScanState;
        const auto pos = state.rowIdxVector->state->getSelVector().getSelectedPositions()[i];
        KU_ASSERT(!state.rowIdxVector->isNull(pos));
        const auto rowIdx = state.rowIdxVector->getValue<row_idx_t>(pos);
        const ChunkedNodeGroup* chunkedGroupToScan = findChunkedGroupFromRowIdx(lock, rowIdx);
        const auto rowIdxInChunkedGroup = rowIdx - chunkedGroupToScan->getStartRowIdx();
        numTuplesFound += chunkedGroupToScan->lookup(transaction, state, nodeGroupScanState,
            rowIdxInChunkedGroup, i);
    }
    return numTuplesFound == state.rowIdxVector->state->getSelVector().getSelSize();
}

bool NodeGroup::lookup(const Transaction* transaction, const TableScanState& state,
    sel_t posInSel) const {
    const auto lock = chunkedGroups.lock();
    return lookup(lock, transaction, state, posInSel);
}

bool NodeGroup::lookupMultiple(const Transaction* transaction, const TableScanState& state) const {
    const auto lock = chunkedGroups.lock();
    return lookupMultiple(lock, transaction, state);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void NodeGroup::update(const Transaction* transaction, row_idx_t rowIdxInGroup,
    column_id_t columnID, const ValueVector& propertyVector) {
    KU_ASSERT(propertyVector.state->getSelVector().getSelSize() == 1);
    ChunkedNodeGroup* chunkedGroupToUpdate = nullptr;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroupToUpdate = findChunkedGroupFromRowIdx(lock, rowIdxInGroup);
    }
    KU_ASSERT(chunkedGroupToUpdate);
    const auto rowIdxInChunkedGroup = rowIdxInGroup - chunkedGroupToUpdate->getStartRowIdx();
    chunkedGroupToUpdate->update(transaction, rowIdxInChunkedGroup, columnID, propertyVector);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
bool NodeGroup::delete_(const Transaction* transaction, row_idx_t rowIdxInGroup) {
    ChunkedNodeGroup* groupToDelete = nullptr;
    {
        const auto lock = chunkedGroups.lock();
        groupToDelete = findChunkedGroupFromRowIdx(lock, rowIdxInGroup);
    }
    const auto rowIdxInChunkedGroup = rowIdxInGroup - groupToDelete->getStartRowIdx();
    return groupToDelete->delete_(transaction, rowIdxInChunkedGroup);
}

bool NodeGroup::hasDeletions(const Transaction* transaction) const {
    const auto lock = chunkedGroups.lock();
    for (auto i = 0u; i < chunkedGroups.getNumGroups(lock); i++) {
        const auto chunkedGroup = chunkedGroups.getGroup(lock, i);
        if (chunkedGroup->hasDeletions(transaction)) {
            return true;
        }
    }
    return false;
}

void NodeGroup::addColumn(Transaction* transaction, TableAddColumnState& addColumnState,
    FileHandle* dataFH, ColumnStats* newColumnStats) {
    dataTypes.push_back(addColumnState.propertyDefinition.getType().copy());
    const auto lock = chunkedGroups.lock();
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->addColumn(transaction, addColumnState, enableCompression, dataFH,
            newColumnStats);
    }
}

void NodeGroup::flush(const Transaction* transaction, FileHandle& dataFH) {
    const auto lock = chunkedGroups.lock();
    if (chunkedGroups.getNumGroups(lock) == 1) {
        const auto chunkedGroupToFlush = chunkedGroups.getFirstGroup(lock);
        chunkedGroupToFlush->flush(dataFH);
    } else {
        // Merge all chunkedGroups into a single one first. Then flush it to disk.
        auto mergedChunkedGroup = std::make_unique<ChunkedNodeGroup>(
            *transaction->getClientContext()->getMemoryManager(), dataTypes, enableCompression,
            StorageConfig::NODE_GROUP_SIZE, getStartRowIdxInGroup(lock), ResidencyState::IN_MEMORY);
        std::vector<column_id_t> dummyColumnIDs;
        for (auto i = 0u; i < dataTypes.size(); i++) {
            dummyColumnIDs.push_back(i);
        }
        for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
            mergedChunkedGroup->append(transaction, dummyColumnIDs, *chunkedGroup, 0,
                chunkedGroup->getNumRows());
        }
        mergedChunkedGroup->flush(dataFH);
        chunkedGroups.replaceGroup(lock, 0, std::move(mergedChunkedGroup));
    }
    // Clear all chunkedGroups except the first one, which is persistent.
    chunkedGroups.resize(lock, 1);
}

void NodeGroup::rollbackInsert(row_idx_t startRow) {
    const auto lock = chunkedGroups.lock();
    const auto numEmptyTrailingGroups = chunkedGroups.getNumEmptyTrailingGroups(lock);
    chunkedGroups.removeTrailingGroups(lock, numEmptyTrailingGroups);
    numRows = startRow;
}

void NodeGroup::reclaimStorage(PageManager& pageManager) const {
    reclaimStorage(pageManager, chunkedGroups.lock());
}

void NodeGroup::reclaimStorage(PageManager& pageManager, const UniqLock& lock) const {
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->reclaimStorage(pageManager);
    }
}

void NodeGroup::checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state) {
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(chunkedGroups.getNumGroups(lock) >= 1);
    const auto firstGroup = chunkedGroups.getFirstGroup(lock);
    const auto hasPersistentData = firstGroup->getResidencyState() == ResidencyState::ON_DISK;
    // Re-populate version info here first.
    auto checkpointedVersionInfo = checkpointVersionInfo(lock, &DUMMY_CHECKPOINT_TRANSACTION);
    std::unique_ptr<ChunkedNodeGroup> checkpointedChunkedGroup;
    if (checkpointedVersionInfo->getNumDeletions(&DUMMY_CHECKPOINT_TRANSACTION, 0, numRows) ==
        numRows - firstGroup->getStartRowIdx()) {
        reclaimStorage(*state.dataFH.getPageManager(), lock);
        // TODO(Royi) figure out how to make this rollback-friendly
        checkpointedChunkedGroup =
            std::make_unique<ChunkedNodeGroup>(memoryManager, dataTypes, enableCompression,
                StorageConfig::CHUNKED_NODE_GROUP_CAPACITY, numRows, ResidencyState::IN_MEMORY);
        checkpointedChunkedGroup->flush(state.dataFH);
    } else {
        if (hasPersistentData) {
            checkpointedChunkedGroup = checkpointInMemAndOnDisk(memoryManager, lock, state);
        } else {
            checkpointedChunkedGroup = checkpointInMemOnly(memoryManager, lock, state);
        }
        checkpointedChunkedGroup->setVersionInfo(std::move(checkpointedVersionInfo));
    }
    chunkedGroups.clear(lock);
    chunkedGroups.appendGroup(lock, std::move(checkpointedChunkedGroup));
    checkpointDataTypesNoLock(state);
}

void NodeGroup::checkpointDataTypesNoLock(const NodeGroupCheckpointState& state) {
    std::vector<LogicalType> checkpointedTypes;
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        auto columnID = state.columnIDs[i];
        KU_ASSERT(columnID < dataTypes.size());
        checkpointedTypes.push_back(dataTypes[columnID].copy());
    }
    dataTypes = std::move(checkpointedTypes);
}

std::unique_ptr<ChunkedNodeGroup> NodeGroup::checkpointInMemAndOnDisk(MemoryManager& memoryManager,
    const UniqLock& lock, NodeGroupCheckpointState& state) const {
    const auto firstGroup = chunkedGroups.getFirstGroup(lock);
    const auto numPersistentRows = firstGroup->getNumRows();
    std::vector<const Column*> columnPtrs;
    columnPtrs.reserve(state.columns.size());
    for (auto* column : state.columns) {
        columnPtrs.push_back(column);
    }
    const auto insertChunkedGroup = scanAllInsertedAndVersions<ResidencyState::IN_MEMORY>(
        memoryManager, lock, state.columnIDs, columnPtrs);
    const auto numInsertedRows = insertChunkedGroup->getNumRows();
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        const auto columnID = state.columnIDs[i];
        // if has persistent data, scan updates from persistent chunked group;
        KU_ASSERT(firstGroup && firstGroup->getResidencyState() == ResidencyState::ON_DISK);
        const auto columnHasUpdates = firstGroup->hasAnyUpdates(&DUMMY_CHECKPOINT_TRANSACTION,
            columnID, 0, firstGroup->getNumRows());
        if (numInsertedRows == 0 && !columnHasUpdates) {
            continue;
        }
        std::vector<ChunkCheckpointState> chunkCheckpointStates;
        if (columnHasUpdates) {
            // TODO(Guodong): Optimize this to scan only vectors with updates.
            const auto updateChunk = scanAllInsertedAndVersions<ResidencyState::ON_DISK>(
                memoryManager, lock, {columnID}, {state.columns[columnID]});
            KU_ASSERT(updateChunk->getNumRows() == numPersistentRows);
            chunkCheckpointStates.push_back(ChunkCheckpointState{
                updateChunk->getColumnChunk(0).moveData(), 0, updateChunk->getNumRows()});
        }
        if (numInsertedRows > 0) {
            chunkCheckpointStates.push_back(
                ChunkCheckpointState{insertChunkedGroup->getColumnChunk(columnID).moveData(),
                    numPersistentRows, numInsertedRows});
        }
        ColumnCheckpointState columnCheckpointState(firstGroup->getColumnChunk(columnID).getData(),
            std::move(chunkCheckpointStates));
        state.columns[i]->checkpointColumnChunk(columnCheckpointState);
    }
    auto checkpointedChunkedGroup =
        std::make_unique<ChunkedNodeGroup>(*chunkedGroups.getGroup(lock, 0), state.columnIDs);
    KU_ASSERT(checkpointedChunkedGroup->getResidencyState() == ResidencyState::ON_DISK);
    checkpointedChunkedGroup->resetNumRowsFromChunks();
    checkpointedChunkedGroup->resetVersionAndUpdateInfo();
    // The first chunked group is the only persistent one
    // The checkpointed columns have been moved to the checkpointedChunkedGroup, the
    // remaining must have been dropped
    chunkedGroups.getGroup(lock, 0)->reclaimStorage(*state.dataFH.getPageManager());
    return checkpointedChunkedGroup;
}

std::unique_ptr<ChunkedNodeGroup> NodeGroup::checkpointInMemOnly(MemoryManager& memoryManager,
    const UniqLock& lock, const NodeGroupCheckpointState& state) const {
    // Flush insertChunkedGroup to persistent one.
    std::vector<const Column*> columnPtrs;
    columnPtrs.reserve(state.columns.size());
    for (auto& column : state.columns) {
        columnPtrs.push_back(column);
    }
    auto insertChunkedGroup = scanAllInsertedAndVersions<ResidencyState::IN_MEMORY>(memoryManager,
        lock, state.columnIDs, columnPtrs);
    insertChunkedGroup->flush(state.dataFH);
    return insertChunkedGroup;
}

std::unique_ptr<VersionInfo> NodeGroup::checkpointVersionInfo(const UniqLock& lock,
    const Transaction* transaction) const {
    auto checkpointVersionInfo = std::make_unique<VersionInfo>();
    row_idx_t currRow = 0;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->hasVersionInfo()) {
            // TODO(Guodong): Optimize the for loop here to directly acess the version info.
            for (auto i = 0u; i < chunkedGroup->getNumRows(); i++) {
                if (chunkedGroup->isDeleted(transaction, i)) {
                    checkpointVersionInfo->delete_(transaction->getID(), currRow + i);
                }
            }
        }
        currRow += chunkedGroup->getNumRows();
    }
    return checkpointVersionInfo;
}

uint64_t NodeGroup::getEstimatedMemoryUsage() const {
    uint64_t memUsage = 0;
    const auto lock = chunkedGroups.lock();
    for (const auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        memUsage += chunkedGroup->getEstimatedMemoryUsage();
    }
    return memUsage;
}

void NodeGroup::serialize(Serializer& serializer) {
    // Serialize checkpointed chunks.
    serializer.writeDebuggingInfo("node_group_idx");
    serializer.write<node_group_idx_t>(nodeGroupIdx);
    serializer.writeDebuggingInfo("enable_compression");
    serializer.write<bool>(enableCompression);
    serializer.writeDebuggingInfo("format");
    serializer.write<NodeGroupDataFormat>(format);
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(chunkedGroups.getNumGroups(lock) == 1);
    const auto chunkedGroup = chunkedGroups.getFirstGroup(lock);
    serializer.writeDebuggingInfo("has_checkpointed_data");
    serializer.write<bool>(chunkedGroup->getResidencyState() == ResidencyState::ON_DISK);
    if (chunkedGroup->getResidencyState() == ResidencyState::ON_DISK) {
        serializer.writeDebuggingInfo("checkpointed_data");
        chunkedGroup->serialize(serializer);
    }
}

std::unique_ptr<NodeGroup> NodeGroup::deserialize(MemoryManager& memoryManager, Deserializer& deSer,
    const std::vector<LogicalType>& columnTypes) {
    std::string key;
    node_group_idx_t nodeGroupIdx = INVALID_NODE_GROUP_IDX;
    bool enableCompression = false;
    auto format = NodeGroupDataFormat::REGULAR;
    bool hasCheckpointedData = false;
    deSer.validateDebuggingInfo(key, "node_group_idx");
    deSer.deserializeValue<node_group_idx_t>(nodeGroupIdx);
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    deSer.validateDebuggingInfo(key, "format");
    deSer.deserializeValue<NodeGroupDataFormat>(format);
    deSer.validateDebuggingInfo(key, "has_checkpointed_data");
    deSer.deserializeValue<bool>(hasCheckpointedData);
    if (hasCheckpointedData) {
        deSer.validateDebuggingInfo(key, "checkpointed_data");
    }
    std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup;
    switch (format) {
    case NodeGroupDataFormat::REGULAR: {
        if (hasCheckpointedData) {
            chunkedNodeGroup = ChunkedNodeGroup::deserialize(memoryManager, deSer);
        } else {
            chunkedNodeGroup = std::make_unique<ChunkedNodeGroup>(memoryManager, columnTypes,
                enableCompression, 0, 0, ResidencyState::IN_MEMORY);
        }
        return std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
            std::move(chunkedNodeGroup));
    }
    case NodeGroupDataFormat::CSR: {
        if (hasCheckpointedData) {
            chunkedNodeGroup = ChunkedCSRNodeGroup::deserialize(memoryManager, deSer);
            return std::make_unique<CSRNodeGroup>(nodeGroupIdx, enableCompression,
                std::move(chunkedNodeGroup));
        } else {
            return std::make_unique<CSRNodeGroup>(nodeGroupIdx, enableCompression,
                copyVector(columnTypes));
        }
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::pair<idx_t, row_idx_t> NodeGroup::findChunkedGroupIdxFromRowIdxNoLock(row_idx_t rowIdx) const {
    if (chunkedGroups.getNumGroupsNoLock() == 0 || rowIdx < getStartRowIdxInGroupNoLock()) {
        return {INVALID_CHUNKED_GROUP_IDX, INVALID_START_ROW_IDX};
    }
    rowIdx -= getStartRowIdxInGroupNoLock();
    const auto numRowsInFirstGroup = chunkedGroups.getFirstGroupNoLock()->getNumRows();
    if (rowIdx < numRowsInFirstGroup) {
        return {0, rowIdx};
    }
    rowIdx -= numRowsInFirstGroup;
    const auto chunkedGroupIdx = rowIdx / StorageConfig::CHUNKED_NODE_GROUP_CAPACITY + 1;
    const auto rowIdxInChunk = rowIdx % StorageConfig::CHUNKED_NODE_GROUP_CAPACITY;
    if (chunkedGroupIdx >= chunkedGroups.getNumGroupsNoLock()) {
        return {INVALID_CHUNKED_GROUP_IDX, INVALID_START_ROW_IDX};
    }
    return {chunkedGroupIdx, rowIdxInChunk};
}

ChunkedNodeGroup* NodeGroup::findChunkedGroupFromRowIdx(const UniqLock& lock,
    row_idx_t rowIdx) const {
    const auto [chunkedGroupIdx, rowIdxInChunkedGroup] =
        findChunkedGroupIdxFromRowIdxNoLock(rowIdx);
    if (chunkedGroupIdx == INVALID_CHUNKED_GROUP_IDX) {
        return nullptr;
    }
    return chunkedGroups.getGroup(lock, chunkedGroupIdx);
}

ChunkedNodeGroup* NodeGroup::findChunkedGroupFromRowIdxNoLock(row_idx_t rowIdx) const {
    const auto [chunkedGroupIdx, rowIdxInChunkedGroup] =
        findChunkedGroupIdxFromRowIdxNoLock(rowIdx);
    if (chunkedGroupIdx == INVALID_CHUNKED_GROUP_IDX) {
        return nullptr;
    }
    return chunkedGroups.getGroupNoLock(chunkedGroupIdx);
}

template<ResidencyState RESIDENCY_STATE>
row_idx_t NodeGroup::getNumResidentRows(const UniqLock& lock) const {
    row_idx_t numResidentRows = 0u;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->getResidencyState() == RESIDENCY_STATE) {
            numResidentRows += chunkedGroup->getNumRows();
        }
    }
    return numResidentRows;
}

template<ResidencyState RESIDENCY_STATE>
std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanAllInsertedAndVersions(
    MemoryManager& memoryManager, const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<const Column*>& columns) const {
    auto numResidentRows = getNumResidentRows<RESIDENCY_STATE>(lock);
    std::vector<LogicalType> columnTypes;
    for (const auto* column : columns) {
        columnTypes.push_back(column->getDataType().copy());
    }
    auto mergedInMemGroup = std::make_unique<ChunkedNodeGroup>(memoryManager, columnTypes,
        enableCompression, numResidentRows, chunkedGroups.getFirstGroup(lock)->getStartRowIdx(),
        ResidencyState::IN_MEMORY);
    auto scanState = std::make_unique<TableScanState>(columnIDs, columns);
    scanState->nodeGroupScanState = std::make_unique<NodeGroupScanState>(columnIDs.size());
    initializeScanState(&DUMMY_CHECKPOINT_TRANSACTION, lock, *scanState);
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->scanCommitted<RESIDENCY_STATE>(&DUMMY_CHECKPOINT_TRANSACTION, *scanState,
            *mergedInMemGroup);
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] != 0) {
            KU_ASSERT(numResidentRows == mergedInMemGroup->getColumnChunk(i).getNumValues());
        }
    }
    mergedInMemGroup->setNumRows(numResidentRows);
    return mergedInMemGroup;
}

template std::unique_ptr<ChunkedNodeGroup>
NodeGroup::scanAllInsertedAndVersions<ResidencyState::ON_DISK>(MemoryManager& memoryManager,
    const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<const Column*>& columns) const;
template std::unique_ptr<ChunkedNodeGroup>
NodeGroup::scanAllInsertedAndVersions<ResidencyState::IN_MEMORY>(MemoryManager& memoryManager,
    const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<const Column*>& columns) const;

bool NodeGroup::isVisible(const Transaction* transaction, row_idx_t rowIdxInGroup) const {
    ChunkedNodeGroup* chunkedGroup = nullptr;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroup = findChunkedGroupFromRowIdx(lock, rowIdxInGroup);
    }
    if (!chunkedGroup) {
        return false;
    }
    const auto rowIdxInChunkedGroup = rowIdxInGroup - chunkedGroup->getStartRowIdx();
    return !chunkedGroup->isDeleted(transaction, rowIdxInChunkedGroup) &&
           chunkedGroup->isInserted(transaction, rowIdxInChunkedGroup);
}

bool NodeGroup::isVisibleNoLock(const Transaction* transaction, row_idx_t rowIdxInGroup) const {
    const auto* chunkedGroup = findChunkedGroupFromRowIdxNoLock(rowIdxInGroup);
    if (!chunkedGroup) {
        return false;
    }
    const auto rowIdxInChunkedGroup = rowIdxInGroup - chunkedGroup->getStartRowIdx();
    return !chunkedGroup->isDeleted(transaction, rowIdxInChunkedGroup) &&
           chunkedGroup->isInserted(transaction, rowIdxInChunkedGroup);
}

bool NodeGroup::isDeleted(const Transaction* transaction, offset_t offsetInGroup) const {
    const auto lock = chunkedGroups.lock();
    const auto* chunkedGroup = findChunkedGroupFromRowIdx(lock, offsetInGroup);
    return chunkedGroup->isDeleted(transaction, offsetInGroup - chunkedGroup->getStartRowIdx());
}

bool NodeGroup::isInserted(const Transaction* transaction, offset_t offsetInGroup) const {
    const auto lock = chunkedGroups.lock();
    const auto* chunkedGroup = findChunkedGroupFromRowIdx(lock, offsetInGroup);
    return chunkedGroup->isInserted(transaction, offsetInGroup - chunkedGroup->getStartRowIdx());
}

void NodeGroup::applyFuncToChunkedGroups(version_record_handler_op_t func, row_idx_t startRow,
    row_idx_t numRows, transaction_t commitTS) const {
    KU_ASSERT(startRow <= getNumRows());

    auto lock = chunkedGroups.lock();
    const auto [chunkedGroupIdx, startRowInChunkedGroup] =
        findChunkedGroupIdxFromRowIdxNoLock(startRow);
    if (chunkedGroupIdx != INVALID_CHUNKED_GROUP_IDX) {
        auto curChunkedGroupIdx = chunkedGroupIdx;
        auto curStartRowIdxInChunk = startRowInChunkedGroup;

        auto numRowsLeft = numRows;
        while (numRowsLeft > 0 && curChunkedGroupIdx < chunkedGroups.getNumGroups(lock)) {
            auto* chunkedGroup = chunkedGroups.getGroup(lock, curChunkedGroupIdx);
            const auto numRowsForGroup =
                std::min(numRowsLeft, chunkedGroup->getNumRows() - curStartRowIdxInChunk);
            std::invoke(func, *chunkedGroup, curStartRowIdxInChunk, numRowsForGroup, commitTS);

            ++curChunkedGroupIdx;
            numRowsLeft -= numRowsForGroup;
            curStartRowIdxInChunk = 0;
        }
    }
}

row_idx_t NodeGroup::getStartRowIdxInGroupNoLock() const {
    return chunkedGroups.getFirstGroupNoLock()->getStartRowIdx();
}

row_idx_t NodeGroup::getStartRowIdxInGroup(const common::UniqLock& lock) const {
    return chunkedGroups.getFirstGroup(lock)->getStartRowIdx();
}

} // namespace storage
} // namespace kuzu
