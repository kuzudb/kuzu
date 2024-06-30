#include "storage/store/node_group.h"

#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::append(const Transaction* transaction, ChunkedNodeGroup& chunkedGroup,
    row_idx_t numRowsToAppend) {
    KU_ASSERT(numRowsToAppend <= chunkedGroup.getNumRows());
    std::vector<ColumnChunk*> chunksToAppend(chunkedGroup.getNumColumns());
    for (auto i = 0u; i < chunkedGroup.getNumColumns(); i++) {
        chunksToAppend[i] = &chunkedGroup.getColumnChunk(i);
    }
    return append(transaction, chunksToAppend, numRowsToAppend);
}

row_idx_t NodeGroup::append(const Transaction* transaction,
    const std::vector<ColumnChunk*>& chunkedGroup, row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows();
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.getLastGroup(lock)->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    ResidencyState::IN_MEMORY));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.getLastGroup(lock);
        KU_ASSERT(ChunkedNodeGroup::CHUNK_CAPACITY >= chunkedGroupToCopyInto->getNumRows());
        auto numToCopyIntoChunk =
            ChunkedNodeGroup::CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk =
            std::min(numRowsToAppend - numRowsAppended, numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(transaction, chunkedGroup, numRowsAppended,
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
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0;
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.getLastGroup(lock)->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    ResidencyState::IN_MEMORY));
        }
        const auto& lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            ChunkedNodeGroup::CHUNK_CAPACITY - lastChunkedGroup->getNumRows());
        lastChunkedGroup->append(transaction, vectors, startRowIdx + numRowsAppended,
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

void NodeGroup::initializeScanState(Transaction*, TableScanState& state) {
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    nodeGroupScanState.chunkedGroupIdx = 0;
    nodeGroupScanState.nextRowToScan = 0;
    ChunkedNodeGroup* firstChunkedGroup;
    {
        const auto lock = chunkedGroups.lock();
        firstChunkedGroup = chunkedGroups.getFirstGroup(lock);
    }
    if (firstChunkedGroup != nullptr &&
        firstChunkedGroup->getResidencyState() == ResidencyState::ON_DISK) {
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            KU_ASSERT(i < state.columnIDs.size());
            KU_ASSERT(i < nodeGroupScanState.chunkStates.size());
            const auto columnID = state.columnIDs[i];
            auto& chunk = firstChunkedGroup->getColumnChunk(columnID);
            chunk.initializeScanState(nodeGroupScanState.chunkStates[i]);
            // TODO: Not a good way to initialize column for chunkState here.
            nodeGroupScanState.chunkStates[i].column = state.columns[i];
        }
    }
}

NodeGroupScanResult NodeGroup::scan(Transaction* transaction, TableScanState& state) {
    // TODO(Guodong): We shouldn't grab lock during scan, instead should do it during
    // initializeScan.
    const auto lock = chunkedGroups.lock();
    auto& nodeGroupScanState = *state.nodeGroupScanState;
    KU_ASSERT(nodeGroupScanState.chunkedGroupIdx < chunkedGroups.getNumGroups(lock));
    if (const auto chunkedGroup = chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
        nodeGroupScanState.nextRowToScan >=
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
    KU_ASSERT(rowIdxInChunkToScan < chunkedGroupToScan.getNumRows());
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - rowIdxInChunkToScan, DEFAULT_VECTOR_CAPACITY);
    chunkedGroupToScan.scan(transaction, state, nodeGroupScanState, rowIdxInChunkToScan,
        numRowsToScan);
    const auto startRow = nodeGroupScanState.nextRowToScan;
    nodeGroupScanState.nextRowToScan += numRowsToScan;
    return NodeGroupScanResult{startRow, numRowsToScan};
}

bool NodeGroup::lookup(Transaction* transaction, const TableScanState& state) {
    idx_t numTuplesFound = 0;
    const auto lock = chunkedGroups.lock();
    for (auto i = 0u; i < state.rowIdxVector->state->getSelVector().getSelSize(); i++) {
        auto& nodeGroupScanState = *state.nodeGroupScanState;
        const auto pos = state.rowIdxVector->state->getSelVector().getSelectedPositions()[0];
        KU_ASSERT(!state.rowIdxVector->isNull(pos));
        const auto rowIdx = state.rowIdxVector->getValue<row_idx_t>(pos);
        const ChunkedNodeGroup* chunkedGroupToScan =
            chunkedGroups.getGroup(lock, rowIdx / ChunkedNodeGroup::CHUNK_CAPACITY);
        numTuplesFound += chunkedGroupToScan->lookup(transaction, state, nodeGroupScanState,
            rowIdx % ChunkedNodeGroup::CHUNK_CAPACITY, i);
    }
    return numTuplesFound > 0;
}

void NodeGroup::update(Transaction* transaction, row_idx_t rowIdxInGroup, column_id_t columnID,
    const ValueVector& propertyVector) {
    KU_ASSERT(propertyVector.state->getSelVector().getSelSize() == 1);
    ChunkedNodeGroup* chunkedGroupToUpdate;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroupToUpdate =
            chunkedGroups.getGroup(lock, rowIdxInGroup / ChunkedNodeGroup::CHUNK_CAPACITY);
    }
    chunkedGroupToUpdate->update(transaction, rowIdxInGroup, columnID, propertyVector);
}

bool NodeGroup::delete_(const Transaction* transaction, row_idx_t rowIdxInGroup) {
    const auto chunkIdx = rowIdxInGroup / ChunkedNodeGroup::CHUNK_CAPACITY;
    const auto rowIdxInChunk = rowIdxInGroup % ChunkedNodeGroup::CHUNK_CAPACITY;
    ChunkedNodeGroup* groupToDelete;
    {
        const auto lock = chunkedGroups.lock();
        groupToDelete = chunkedGroups.getGroup(lock, chunkIdx);
    }
    return groupToDelete->delete_(transaction, rowIdxInChunk);
}

void NodeGroup::flush(BMFileHandle& dataFH) {
    const auto lock = chunkedGroups.lock();
    if (chunkedGroups.getNumGroups(lock) == 1) {
        const auto chunkedGroupToFlush = chunkedGroups.getGroup(lock, 0);
        auto flushedChunkedGroup = chunkedGroupToFlush->flush(dataFH);
        chunkedGroups.replaceGroup(lock, 0, std::move(flushedChunkedGroup));
    } else {
        // Merge all chunkedGroups into a single one first. Then flush it to disk.
        const auto mergedChunkedGroup = std::make_unique<ChunkedNodeGroup>(dataTypes,
            enableCompression, StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
        for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
            mergedChunkedGroup->append(&DUMMY_WRITE_TRANSACTION, *chunkedGroup, 0,
                chunkedGroup->getNumRows());
        }
        auto flushedChunkedGroup = mergedChunkedGroup->flush(dataFH);
        chunkedGroups.replaceGroup(lock, 0, std::move(flushedChunkedGroup));
    }
}

void NodeGroup::checkpoint(const NodeGroupCheckpointState& state) {
    // We don't need to consider deletions here, as they are flushed separately as metadata.
    // A special but rare case can be all rows are deleted, then we can skip flushing the data.
    const auto updateChunkedGroup =
        scanCommitted<ResidencyState::ON_DISK>(state.columnIDs, state.columns);
    const auto insertChunkedGroup =
        scanCommitted<ResidencyState::IN_MEMORY>(state.columnIDs, state.columns);
    std::vector<std::unique_ptr<ColumnChunk>> checkpointedChunks;
    checkpointedChunks.resize(state.columns.size());
    const auto dummyTransaction = std::make_unique<Transaction>(TransactionType::READ_ONLY, 0,
        Transaction::START_TRANSACTION_ID - 1);
    for (auto columnID = 0u; columnID < state.columns.size(); columnID++) {
        checkpointedChunks[columnID] = state.columns[columnID]->checkpointColumnChunk(
            insertChunkedGroup->getColumnChunk(columnID),
            updateChunkedGroup->getColumnChunk(columnID));
    }
    auto checkpointedGroup = std::make_unique<ChunkedNodeGroup>(std::move(checkpointedChunks), 0);
    const auto lock = chunkedGroups.lock();
    chunkedGroups.clear(lock);
    chunkedGroups.appendGroup(lock, std::move(checkpointedGroup));
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
    serializer.write<node_group_idx_t>(nodeGroupIdx);
    serializer.write<bool>(enableCompression);
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(chunkedGroups.getNumGroups(lock) == 1);
    const auto chunkedGroup = chunkedGroups.getFirstGroup(lock);
    serializer.write<bool>(chunkedGroup->getResidencyState() == ResidencyState::ON_DISK);
    if (chunkedGroup->getResidencyState() == ResidencyState::ON_DISK) {
        chunkedGroup->serialize(serializer);
    }
}

std::unique_ptr<NodeGroup> NodeGroup::deserialize(Deserializer& deSer) {
    node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    bool hasCheckpointedData;
    deSer.deserializeValue<node_group_idx_t>(nodeGroupIdx);
    deSer.deserializeValue<bool>(enableCompression);
    deSer.deserializeValue<bool>(hasCheckpointedData);
    if (!hasCheckpointedData) {
        return nullptr;
    }
    auto chunkedNodeGroup = ChunkedNodeGroup::deserialize(deSer);
    return std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
        std::move(chunkedNodeGroup));
}

template<ResidencyState SCAN_RESIDENCY_STATE>
std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted(
    const std::vector<column_id_t>& columnIDs, const std::vector<Column*>& columns) {
    // TODO(Guodong): capacity should be set based on numValues.
    auto mergedInMemGroup = std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
        StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
    const auto dummyTransaction = std::make_unique<Transaction>(TransactionType::READ_ONLY, 0,
        Transaction::START_TRANSACTION_ID - 1);
    TableScanState scanState(INVALID_TABLE_ID, columnIDs, columns);
    scanState.nodeGroupScanState = std::make_unique<NodeGroupScanState>(columnIDs.size());
    initializeScanState(dummyTransaction.get(), scanState);
    const auto lock = chunkedGroups.lock();
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->scanCommitted<SCAN_RESIDENCY_STATE>(dummyTransaction.get(), scanState,
            *scanState.nodeGroupScanState, *mergedInMemGroup);
    }
    // TODO: mergedInMemGroup is not set numRows.
    return mergedInMemGroup;
}

template std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted<ResidencyState::ON_DISK>(
    const std::vector<column_id_t>& columnIDs, const std::vector<Column*>& columns);
template std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted<ResidencyState::IN_MEMORY>(
    const std::vector<column_id_t>& columnIDs, const std::vector<Column*>& columns);

} // namespace storage
} // namespace kuzu
