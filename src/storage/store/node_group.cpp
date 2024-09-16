#include "storage/store/node_group.h"

#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/csr_node_group.h"
#include "storage/store/table.h"
#include "transaction/transaction.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::append(const Transaction* transaction, ChunkedNodeGroup& chunkedGroup,
    row_idx_t startRowIdx, row_idx_t numRowsToAppend) {
    KU_ASSERT(numRowsToAppend <= chunkedGroup.getNumRows());
    std::vector<ColumnChunk*> chunksToAppend(chunkedGroup.getNumColumns());
    for (auto i = 0u; i < chunkedGroup.getNumColumns(); i++) {
        chunksToAppend[i] = &chunkedGroup.getColumnChunk(i);
    }
    return append(transaction, chunksToAppend, startRowIdx, numRowsToAppend);
}

row_idx_t NodeGroup::append(const Transaction* transaction,
    const std::vector<ColumnChunk*>& chunkedGroup, row_idx_t startRowIdx,
    row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows();
    auto& mm = *transaction->getClientContext()->getMemoryManager();
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        auto lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        if (!lastChunkedGroup || lastChunkedGroup->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    ResidencyState::IN_MEMORY));
        }
        lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        KU_ASSERT(ChunkedNodeGroup::CHUNK_CAPACITY >= lastChunkedGroup->getNumRows());
        auto numToCopyIntoChunk = ChunkedNodeGroup::CHUNK_CAPACITY - lastChunkedGroup->getNumRows();
        const auto numToAppendInChunk =
            std::min(numRowsToAppend - numRowsAppended, numToCopyIntoChunk);
        lastChunkedGroup->append(transaction, chunkedGroup, numRowsAppended + startRowIdx,
            numToAppendInChunk);
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
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0;
    while (numRowsAppended < numRowsToAppend) {
        auto lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        if (!lastChunkedGroup || lastChunkedGroup->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(mm, dataTypes, enableCompression,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    ResidencyState::IN_MEMORY));
        }
        lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            ChunkedNodeGroup::CHUNK_CAPACITY - lastChunkedGroup->getNumRows());
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

void NodeGroup::initializeScanState(Transaction* transaction, TableScanState& state) {
    const auto lock = chunkedGroups.lock();
    initializeScanState(transaction, lock, state);
}

void NodeGroup::initializeScanState(Transaction*, const UniqLock& lock, TableScanState& state) {
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    nodeGroupScanState.chunkedGroupIdx = 0;
    nodeGroupScanState.nextRowToScan = 0;
    ChunkedNodeGroup* firstChunkedGroup = chunkedGroups.getFirstGroup(lock);
    if (firstChunkedGroup != nullptr &&
        firstChunkedGroup->getResidencyState() == ResidencyState::ON_DISK) {
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            KU_ASSERT(i < state.columnIDs.size());
            KU_ASSERT(i < nodeGroupScanState.chunkStates.size());
            const auto columnID = state.columnIDs[i];
            if (columnID == INVALID_COLUMN_ID || columnID == ROW_IDX_COLUMN_ID) {
                continue;
            }
            auto& chunk = firstChunkedGroup->getColumnChunk(columnID);
            chunk.initializeScanState(nodeGroupScanState.chunkStates[i], state.columns[i]);
        }
    }
}

NodeGroupScanResult NodeGroup::scan(Transaction* transaction, TableScanState& state) {
    // TODO(Guodong): We shouldn't grab lock during scan, instead should do it during
    // initializeScan.
    const auto lock = chunkedGroups.lock();
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    KU_ASSERT(nodeGroupScanState.chunkedGroupIdx < chunkedGroups.getNumGroups(lock));
    const auto chunkedGroup = chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
    if (chunkedGroup && nodeGroupScanState.nextRowToScan >=
                            chunkedGroup->getNumRows() + chunkedGroup->getStartRowIdx()) {
        nodeGroupScanState.chunkedGroupIdx++;
    }
    if (nodeGroupScanState.chunkedGroupIdx >= chunkedGroups.getNumGroups(lock)) {
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    const auto& chunkedGroupToScan =
        *chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
    const auto rowIdxInChunkToScan =
        nodeGroupScanState.nextRowToScan - chunkedGroupToScan.getStartRowIdx();
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - rowIdxInChunkToScan, DEFAULT_VECTOR_CAPACITY);
    if (state.source == TableScanSource::COMMITTED && state.semiMask &&
        state.semiMask->isEnabled()) {
        const auto startNodeOffset = nodeGroupScanState.nextRowToScan +
                                     StorageUtils::getStartOffsetOfNodeGroup(state.nodeGroupIdx);
        if (!state.semiMask->isMasked(startNodeOffset, startNodeOffset + numRowsToScan - 1)) {
            state.outState->getSelVectorUnsafe().setSelSize(0);
            nodeGroupScanState.nextRowToScan += numRowsToScan;
            return NodeGroupScanResult{nodeGroupScanState.nextRowToScan, 0};
        }
    }
    chunkedGroupToScan.scan(transaction, state, nodeGroupScanState, rowIdxInChunkToScan,
        numRowsToScan);
    const auto startRow = nodeGroupScanState.nextRowToScan;
    nodeGroupScanState.nextRowToScan += numRowsToScan;
    return NodeGroupScanResult{startRow, numRowsToScan};
}

bool NodeGroup::lookup(const UniqLock& lock, Transaction* transaction,
    const TableScanState& state) {
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

bool NodeGroup::lookup(Transaction* transaction, const TableScanState& state) {
    const auto lock = chunkedGroups.lock();
    return lookup(lock, transaction, state);
}

void NodeGroup::update(Transaction* transaction, row_idx_t rowIdxInGroup, column_id_t columnID,
    const ValueVector& propertyVector) {
    KU_ASSERT(propertyVector.state->getSelVector().getSelSize() == 1);
    ChunkedNodeGroup* chunkedGroupToUpdate;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroupToUpdate = findChunkedGroupFromRowIdx(lock, rowIdxInGroup);
    }
    const auto rowIdxInChunkedGroup = rowIdxInGroup - chunkedGroupToUpdate->getStartRowIdx();
    chunkedGroupToUpdate->update(transaction, rowIdxInChunkedGroup, columnID, propertyVector);
}

bool NodeGroup::delete_(const Transaction* transaction, row_idx_t rowIdxInGroup) {
    ChunkedNodeGroup* groupToDelete;
    {
        const auto lock = chunkedGroups.lock();
        groupToDelete = findChunkedGroupFromRowIdx(lock, rowIdxInGroup);
    }
    const auto rowIdxInChunkedGroup = rowIdxInGroup - groupToDelete->getStartRowIdx();
    return groupToDelete->delete_(transaction, rowIdxInChunkedGroup);
}

bool NodeGroup::hasDeletions(const Transaction* transaction) {
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
    FileHandle* dataFH) {
    dataTypes.push_back(addColumnState.propertyDefinition.getType().copy());
    const auto lock = chunkedGroups.lock();
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->addColumn(transaction, addColumnState, enableCompression, dataFH);
    }
}

void NodeGroup::flush(Transaction* transaction, FileHandle& dataFH) {
    const auto lock = chunkedGroups.lock();
    if (chunkedGroups.getNumGroups(lock) == 1) {
        const auto chunkedGroupToFlush = chunkedGroups.getFirstGroup(lock);
        chunkedGroupToFlush->flush(dataFH);
    } else {
        // Merge all chunkedGroups into a single one first. Then flush it to disk.
        auto mergedChunkedGroup = std::make_unique<ChunkedNodeGroup>(
            *transaction->getClientContext()->getMemoryManager(), dataTypes, enableCompression,
            StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
        for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
            mergedChunkedGroup->append(transaction, *chunkedGroup, 0, chunkedGroup->getNumRows());
        }
        mergedChunkedGroup->flush(dataFH);
        chunkedGroups.replaceGroup(lock, 0, std::move(mergedChunkedGroup));
    }
    // Clear all chunkedGroups except the first one, which is persistent.
    chunkedGroups.resize(lock, 1);
}

void NodeGroup::checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state) {
    // We don't need to consider deletions here, as they are flushed separately as metadata.
    // TODO(Guodong): A special case can be all rows are deleted or rollbacked, then we can skip
    // flushing the data.
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(chunkedGroups.getNumGroups(lock) >= 1);
    const auto firstGroup = chunkedGroups.getFirstGroup(lock);
    const auto hasPersistentData = firstGroup->getResidencyState() == ResidencyState::ON_DISK;
    // Re-populate version info here first.
    auto checkpointedVersionInfo = checkpointVersionInfo(lock, &DUMMY_CHECKPOINT_TRANSACTION);
    std::unique_ptr<ChunkedNodeGroup> checkpointedChunkedGroup;
    if (hasPersistentData) {
        checkpointedChunkedGroup = checkpointInMemAndOnDisk(memoryManager, lock, state);
    } else {
        checkpointedChunkedGroup = checkpointInMemOnly(memoryManager, lock, state);
    }
    checkpointedChunkedGroup->setVersionInfo(std::move(checkpointedVersionInfo));
    chunkedGroups.clear(lock);
    chunkedGroups.appendGroup(lock, std::move(checkpointedChunkedGroup));
}

std::unique_ptr<ChunkedNodeGroup> NodeGroup::checkpointInMemAndOnDisk(MemoryManager& memoryManager,
    const UniqLock& lock, NodeGroupCheckpointState& state) {
    const auto firstGroup = chunkedGroups.getFirstGroup(lock);
    const auto numPersistentRows = firstGroup->getNumRows();
    std::vector<Column*> columnPtrs;
    columnPtrs.reserve(state.columns.size());
    for (auto& column : state.columns) {
        columnPtrs.push_back(column.get());
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
                memoryManager, lock, {columnID}, {state.columns[columnID].get()});
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
    // Clear all chunked groups except for the first one.
    auto checkpointedChunkedGroup =
        std::make_unique<ChunkedNodeGroup>(*chunkedGroups.getGroup(lock, 0), state.columnIDs);
    KU_ASSERT(checkpointedChunkedGroup->getResidencyState() == ResidencyState::ON_DISK);
    checkpointedChunkedGroup->resetNumRowsFromChunks();
    return checkpointedChunkedGroup;
}

std::unique_ptr<ChunkedNodeGroup> NodeGroup::checkpointInMemOnly(MemoryManager& memoryManager,
    const UniqLock& lock, NodeGroupCheckpointState& state) {
    // Flush insertChunkedGroup to persistent one.
    std::vector<Column*> columnPtrs;
    columnPtrs.reserve(state.columns.size());
    for (auto& column : state.columns) {
        columnPtrs.push_back(column.get());
    }
    auto insertChunkedGroup = scanAllInsertedAndVersions<ResidencyState::IN_MEMORY>(memoryManager,
        lock, state.columnIDs, columnPtrs);
    insertChunkedGroup->flush(state.dataFH);
    return insertChunkedGroup;
}

std::unique_ptr<VersionInfo> NodeGroup::checkpointVersionInfo(const UniqLock& lock,
    const Transaction* transaction) {
    auto checkpointVersionInfo = std::make_unique<VersionInfo>();
    row_idx_t currRow = 0;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->hasVersionInfo()) {
            // TODO(Guodong): Optimize the for loop here to directly acess the version info.
            for (auto i = 0u; i < chunkedGroup->getNumRows(); i++) {
                if (chunkedGroup->isDeleted(transaction, i)) {
                    checkpointVersionInfo->delete_(transaction, nullptr, currRow + i);
                }
            }
        }
        currRow += chunkedGroup->getNumRows();
    }
    return checkpointVersionInfo;
}

void NodeGroup::populateNodeID(ValueVector& nodeIDVector, table_id_t tableID,
    const offset_t startNodeOffset, const row_idx_t numRows) {
    for (auto i = 0u; i < numRows; i++) {
        nodeIDVector.setValue<nodeID_t>(i, {startNodeOffset + i, tableID});
    }
}

bool NodeGroup::hasChanges() {
    const auto lock = chunkedGroups.lock();
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->hasUpdates()) {
            return true;
        }
    }
    return false;
}

uint64_t NodeGroup::getEstimatedMemoryUsage() {
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

std::unique_ptr<NodeGroup> NodeGroup::deserialize(MemoryManager& memoryManager,
    Deserializer& deSer) {
    std::string key;
    node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    NodeGroupDataFormat format;
    bool hasCheckpointedData;
    deSer.validateDebuggingInfo(key, "node_group_idx");
    deSer.deserializeValue<node_group_idx_t>(nodeGroupIdx);
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    deSer.validateDebuggingInfo(key, "format");
    deSer.deserializeValue<NodeGroupDataFormat>(format);
    deSer.validateDebuggingInfo(key, "has_checkpointed_data");
    deSer.deserializeValue<bool>(hasCheckpointedData);
    if (!hasCheckpointedData) {
        return nullptr;
    }
    deSer.validateDebuggingInfo(key, "checkpointed_data");
    std::unique_ptr<ChunkedNodeGroup> chunkedNodeGroup;
    switch (format) {
    case NodeGroupDataFormat::REGULAR: {
        chunkedNodeGroup = ChunkedNodeGroup::deserialize(memoryManager, deSer);
        return std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
            std::move(chunkedNodeGroup));
    }
    case NodeGroupDataFormat::CSR: {
        chunkedNodeGroup = ChunkedCSRNodeGroup::deserialize(memoryManager, deSer);
        return std::make_unique<CSRNodeGroup>(nodeGroupIdx, enableCompression,
            std::move(chunkedNodeGroup));
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

ChunkedNodeGroup* NodeGroup::findChunkedGroupFromRowIdx(const UniqLock& lock, row_idx_t rowIdx) {
    KU_ASSERT(!chunkedGroups.isEmpty(lock));
    const auto numRowsInFirstGroup = chunkedGroups.getFirstGroup(lock)->getNumRows();
    if (rowIdx < numRowsInFirstGroup) {
        return chunkedGroups.getFirstGroup(lock);
    }
    rowIdx -= numRowsInFirstGroup;
    const auto chunkedGroupIdx = rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY + 1;
    return chunkedGroups.getGroup(lock, chunkedGroupIdx);
}

ChunkedNodeGroup* NodeGroup::findChunkedGroupFromRowIdxNoLock(row_idx_t rowIdx) {
    const auto numRowsInFirstGroup = chunkedGroups.getFirstGroupNoLock()->getNumRows();
    if (rowIdx < numRowsInFirstGroup) {
        return chunkedGroups.getFirstGroupNoLock();
    }
    rowIdx -= numRowsInFirstGroup;
    const auto chunkedGroupIdx = rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY + 1;
    return chunkedGroups.getGroupNoLock(chunkedGroupIdx);
}

template<ResidencyState RESIDENCY_STATE>
row_idx_t NodeGroup::getNumResidentRows(const UniqLock& lock) {
    row_idx_t numRows = 0u;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->getResidencyState() == RESIDENCY_STATE) {
            numRows += chunkedGroup->getNumRows();
        }
    }
    return numRows;
}

template<ResidencyState RESIDENCY_STATE>
std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanAllInsertedAndVersions(
    MemoryManager& memoryManager, const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<Column*>& columns) {
    auto numRows = getNumResidentRows<RESIDENCY_STATE>(lock);
    const auto columnTypes = columns | std::views::transform([](const auto* column) {
        return column->getDataType().copy();
    });
    auto mergedInMemGroup = std::make_unique<ChunkedNodeGroup>(memoryManager,
        std::vector<LogicalType>{columnTypes.begin(), columnTypes.end()}, enableCompression,
        numRows, 0, ResidencyState::IN_MEMORY);
    TableScanState scanState(columnIDs, columns);
    scanState.nodeGroupScanState = std::make_unique<NodeGroupScanState>(columnIDs.size());
    initializeScanState(&DUMMY_CHECKPOINT_TRANSACTION, lock, scanState);
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->scanCommitted<RESIDENCY_STATE>(&DUMMY_CHECKPOINT_TRANSACTION, scanState,
            *scanState.nodeGroupScanState, *mergedInMemGroup);
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] != 0) {
            KU_ASSERT(numRows == mergedInMemGroup->getColumnChunk(i).getNumValues());
        }
    }
    mergedInMemGroup->setNumRows(numRows);
    return mergedInMemGroup;
}

template std::unique_ptr<ChunkedNodeGroup>
NodeGroup::scanAllInsertedAndVersions<ResidencyState::ON_DISK>(MemoryManager& memoryManager,
    const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<Column*>& columns);
template std::unique_ptr<ChunkedNodeGroup>
NodeGroup::scanAllInsertedAndVersions<ResidencyState::IN_MEMORY>(MemoryManager& memoryManager,
    const UniqLock& lock, const std::vector<column_id_t>& columnIDs,
    const std::vector<Column*>& columns);

bool NodeGroup::isVisible(const Transaction* transaction, row_idx_t rowIdxInGroup) {
    const auto* chunkedGroup = findChunkedGroupFromRowIdxNoLock(rowIdxInGroup);
    const auto rowIdxInChunkedGroup = rowIdxInGroup - chunkedGroup->getStartRowIdx();
    return !chunkedGroup->isDeleted(transaction, rowIdxInChunkedGroup) &&
           chunkedGroup->isInserted(transaction, rowIdxInChunkedGroup);
}

bool NodeGroup::isDeleted(const Transaction* transaction, offset_t offsetInGroup) {
    const auto lock = chunkedGroups.lock();
    const auto* chunkedGroup = findChunkedGroupFromRowIdx(lock, offsetInGroup);
    return chunkedGroup->isDeleted(transaction, offsetInGroup - chunkedGroup->getStartRowIdx());
}

bool NodeGroup::isInserted(const Transaction* transaction, offset_t offsetInGroup) {
    const auto lock = chunkedGroups.lock();
    const auto* chunkedGroup = findChunkedGroupFromRowIdx(lock, offsetInGroup);
    return chunkedGroup->isInserted(transaction, offsetInGroup - chunkedGroup->getStartRowIdx());
}

} // namespace storage
} // namespace kuzu
