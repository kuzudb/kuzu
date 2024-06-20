#include "storage/store/node_group.h"

#include "storage/store/node_table.h"
#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::append(const Transaction* transaction, const ChunkedNodeGroup& chunkedGroup,
    row_idx_t numRowsToAppend) {
    KU_ASSERT(numRowsToAppend <= chunkedGroup.getNumRows());
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
        const auto numToAppendInChunk = std::min(chunkedGroup.getNumRows(), numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(transaction, chunkedGroup, 0, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
    }
    numRows += numRowsAppended;
    return numRowsBeforeAppend + startNodeOffset;
}

void NodeGroup::append(const Transaction* transaction, const std::vector<ValueVector*>& vectors,
    const row_idx_t startRowIdx, const row_idx_t numRowsToAppend) {
    // TODO: Remove the assumption of all vectors have the same selVector.
    auto& anchorSelVector = vectors[0]->state->getSelVector();
    SelectionVector selVector(DEFAULT_VECTOR_CAPACITY);
    auto selVectorBuffer = selVector.getMultableBuffer();
    for (auto i = 0u; i < numRowsToAppend; i++) {
        selVectorBuffer[i] = anchorSelVector[startRowIdx + i];
    }
    selVector.setToFiltered(numRowsToAppend);

    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows();
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0;
    SelectionVector tmpSelVector(numRowsToAppend);
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
        auto tmpSelVectorBuffer = tmpSelVector.getMultableBuffer();
        for (auto i = 0u; i < numRowsToAppendInGroup; i++) {
            tmpSelVectorBuffer[i] = selVector[numRowsAppended + i];
        }
        tmpSelVector.setToFiltered(numRowsToAppendInGroup);
        lastChunkedGroup->append(transaction, vectors, tmpSelVector, numRowsToAppendInGroup);
        numRowsAppended += numRowsToAppendInGroup;
    }
}

void NodeGroup::merge(Transaction*, std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == dataTypes.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == dataTypes[i]);
    }
    const auto lock = chunkedGroups.lock();
    chunkedGroups.appendGroup(lock, std::move(chunkedGroup));
}

void NodeGroup::initializeScanState(Transaction*, const TableScanState& state,
    NodeGroupScanState& nodeGroupScanState) {
    nodeGroupScanState.chunkStates.resize(state.columnIDs.size());
    ChunkedNodeGroup* checkpointedChunkedGroup;
    {
        const auto lock = chunkedGroups.lock();
        checkpointedChunkedGroup = chunkedGroups.getFirstGroup(lock);
    }
    if (checkpointedChunkedGroup != nullptr &&
        checkpointedChunkedGroup->getResidencyState() == ResidencyState::ON_DISK) {
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            const auto columnID = state.columnIDs[i];
            auto& chunk = checkpointedChunkedGroup->getColumnChunk(columnID);
            chunk.initializeScanState(nodeGroupScanState.chunkStates[i]);
            // TODO: Not a good way to initialize column for chunkState here.
            nodeGroupScanState.chunkStates[i].column = state.columns[i];
        }
    }
}

bool NodeGroup::scan(Transaction* transaction, TableScanState& state,
    NodeGroupScanState& nodeGroupScanState) {
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(nodeGroupScanState.chunkedGroupIdx < chunkedGroups.getNumGroups(lock));
    if (const auto chunkedGroup = chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
        nodeGroupScanState.nextRowToScan == chunkedGroup->getNumRows()) {
        nodeGroupScanState.chunkedGroupIdx++;
    }
    if (nodeGroupScanState.chunkedGroupIdx >= chunkedGroups.getNumGroups(lock)) {
        state.IDVector->state->getSelVectorUnsafe().setToUnfiltered(0);
        return false;
    }
    const auto& chunkedGroupToScan =
        *chunkedGroups.getGroup(lock, nodeGroupScanState.chunkedGroupIdx);
    const auto offsetToScan =
        nodeGroupScanState.nextRowToScan - chunkedGroupToScan.getStartNodeOffset();
    KU_ASSERT(offsetToScan < chunkedGroupToScan.getNumRows());
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - offsetToScan, DEFAULT_VECTOR_CAPACITY);
    const auto nodeOffset = startNodeOffset + nodeGroupScanState.nextRowToScan;
    populateNodeID(*state.IDVector, state.tableID, nodeOffset, numRowsToScan);
    state.IDVector->state->getSelVectorUnsafe().setToUnfiltered(numRowsToScan);
    chunkedGroupToScan.scan(transaction, state, nodeGroupScanState, offsetToScan, numRowsToScan);
    nodeGroupScanState.nextRowToScan += numRowsToScan;
    return true;
}

void NodeGroup::lookup(Transaction* transaction, TableScanState& state,
    NodeGroupScanState& nodeGroupScanState) {
    KU_ASSERT(state.IDVector->state->getSelVector().getSelSize() == 1);
    const auto pos = state.IDVector->state->getSelVector().getSelectedPositions()[0];
    if (state.IDVector->isNull(pos)) {
        return;
    }
    const auto nodeOffset = state.IDVector->getValue<nodeID_t>(pos).offset;
    const auto offsetInGroup = nodeOffset - startNodeOffset;
    const ChunkedNodeGroup* chunkedGroupToScan;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroupToScan =
            chunkedGroups.getGroup(lock, offsetInGroup / ChunkedNodeGroup::CHUNK_CAPACITY);
    }
    chunkedGroupToScan->lookup(transaction, state, nodeGroupScanState,
        offsetInGroup % ChunkedNodeGroup::CHUNK_CAPACITY);
}

void NodeGroup::update(Transaction* transaction, offset_t offset, column_id_t columnID,
    ValueVector& propertyVector) {
    KU_ASSERT(propertyVector.state->getSelVector().getSelSize() == 1);
    const auto offsetInGroup = offset - startNodeOffset;
    ChunkedNodeGroup* chunkedGroupToUpdate;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroupToUpdate =
            chunkedGroups.getGroup(lock, offsetInGroup / ChunkedNodeGroup::CHUNK_CAPACITY);
    }
    chunkedGroupToUpdate->update(transaction, offsetInGroup, columnID, propertyVector);
}

bool NodeGroup::delete_(Transaction* transaction, const offset_t offset) {
    const auto offsetInGroup = offset - startNodeOffset;
    const auto chunkIdx = offsetInGroup / ChunkedNodeGroup::CHUNK_CAPACITY;
    const auto offsetInChunk = offsetInGroup % ChunkedNodeGroup::CHUNK_CAPACITY;
    ChunkedNodeGroup* groupToDelete;
    {
        const auto lock = chunkedGroups.lock();
        groupToDelete = chunkedGroups.getGroup(lock, chunkIdx);
    }
    return groupToDelete->delete_(transaction, offsetInChunk);
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
    auto checkpointedGroup = std::make_unique<ChunkedNodeGroup>(std::move(checkpointedChunks),
        startNodeOffset, startNodeOffset);
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
        StorageConstants::NODE_GROUP_SIZE, startNodeOffset, ResidencyState::IN_MEMORY);
    const auto dummyTransaction = std::make_unique<Transaction>(TransactionType::READ_ONLY, 0,
        Transaction::START_TRANSACTION_ID - 1);
    TableScanState scanState(INVALID_TABLE_ID, columnIDs, columns);
    NodeGroupScanState nodeGroupScanState;
    initializeScanState(dummyTransaction.get(), scanState, nodeGroupScanState);
    const auto lock = chunkedGroups.lock();
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        chunkedGroup->scanCommitted<SCAN_RESIDENCY_STATE>(dummyTransaction.get(), scanState,
            nodeGroupScanState, *mergedInMemGroup);
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
