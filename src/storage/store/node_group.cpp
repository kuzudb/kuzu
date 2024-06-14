#include "storage/store/node_group.h"

#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void NodeGroup::append(const Transaction* transaction,
    const ChunkedNodeGroupCollection& chunkCollection, const row_idx_t offset,
    const row_idx_t numRowsToAppend) {
    const auto startOffset = chunkedGroups.append(chunkCollection, offset, numRowsToAppend);
    versionInfo.append(transaction, startOffset, numRowsToAppend);
}

offset_t NodeGroup::append(const Transaction* transaction, const ChunkedNodeGroup& chunkedGroup,
    row_idx_t numRowsToAppend) {
    const auto rowIdx = chunkedGroups.append(chunkedGroup, numRowsToAppend);
    versionInfo.append(transaction, rowIdx, numRowsToAppend);
    return rowIdx + startNodeOffset;
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
    const auto startOffset = chunkedGroups.append(vectors, selVector);
    versionInfo.append(transaction, startOffset, numRowsToAppend);
}

void NodeGroup::merge(Transaction*, std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    chunkedGroups.merge(std::move(chunkedGroup));
}

void NodeGroup::initializeScanState(Transaction*, TableScanState& state) const {
    if (getNumChunkedGroups() == 0) {
        return;
    }
    auto& chunkedGroup = chunkedGroups.getChunkedGroup(0);
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        const auto columnID = state.columnIDs[i];
        auto& chunk = chunkedGroup.getColumnChunk(columnID);
        chunk.initializeScanState(state.chunkStates[i]);
        if (residencyState == ResidencyState::ON_DISK) {
            KU_ASSERT(state.tableData);
            state.chunkStates[i].column = state.tableData->getColumn(columnID);
        }
    }
}

bool NodeGroup::scan(Transaction* transaction, TableScanState& state) const {
    auto& nodeGroupState = state.nodeGroupScanState;
    KU_ASSERT(nodeGroupState.chunkedGroupIdx < chunkedGroups.getNumChunkedGroups());
    if (const auto& chunkedGroup = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
        nodeGroupState.nextRowToScan ==
        chunkedGroup.getStartNodeOffset() + chunkedGroup.getNumRows()) {
        nodeGroupState.chunkedGroupIdx++;
    }
    if (nodeGroupState.chunkedGroupIdx >= chunkedGroups.getNumChunkedGroups()) {
        state.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(0);
        return false;
    }
    auto& chunkedGroupToScan = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
    const auto offsetToScan =
        nodeGroupState.nextRowToScan - chunkedGroupToScan.getStartNodeOffset();
    KU_ASSERT(offsetToScan < chunkedGroupToScan.getNumRows());
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - offsetToScan, DEFAULT_VECTOR_CAPACITY);
    SelectionVector selVector(DEFAULT_VECTOR_CAPACITY);
    versionInfo.getSelVectorToScan(transaction->getStartTS(), transaction->getID(), selVector,
        offsetToScan, numRowsToScan);
    const auto nodeOffset = startNodeOffset + nodeGroupState.nextRowToScan;
    populateNodeID(*state.nodeIDVector, selVector, state.tableID, nodeOffset, numRowsToScan);
    if (selVector.getSelSize() > 0) {
        chunkedGroupToScan.scan(transaction, state, offsetToScan, numRowsToScan);
    }
    nodeGroupState.nextRowToScan += numRowsToScan;
    return true;
}

void NodeGroup::lookup(Transaction* transaction, TableScanState& state) const {
    KU_ASSERT(state.nodeIDVector->state->getSelVector().getSelSize() == 1);
    // TODO(Guodong): Handle visbility here.
    switch (residencyState) {
    case ResidencyState::IN_MEMORY:
    case ResidencyState::TEMPORARY: {
        const auto nodeOffset = state.nodeIDVector->getValue<nodeID_t>(0).offset;
        const auto offsetInGroup = nodeOffset - startNodeOffset;
        const auto& chunkedGroupToScan = chunkedGroups.findChunkedGroupFromOffset(offsetInGroup);
        chunkedGroupToScan.lookup(transaction, state.columnIDs, state.outputVectors, offsetInGroup);
    } break;
    case ResidencyState::ON_DISK: {
        KU_ASSERT(state.tableData);
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            const auto columnID = state.columnIDs[i];
            state.tableData->getColumn(columnID)->lookup(transaction, state.chunkStates[i],
                state.nodeIDVector, state.outputVectors[i]);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void NodeGroup::update(Transaction* transaction, offset_t offset, column_id_t columnID,
    ValueVector& propertyVector) {
    KU_ASSERT(propertyVector.state->getSelVector().getSelSize() == 1);
    const auto offsetInGroup = offset - startNodeOffset;
    auto& chunkedGroup = chunkedGroups.findChunkedGroupFromOffset(offsetInGroup);
    chunkedGroup.update(transaction, offsetInGroup, columnID, propertyVector);
}

bool NodeGroup::delete_(Transaction* transaction, const offset_t offset) {
    const auto offsetInGroup = offset - startNodeOffset;
    return versionInfo.delete_(transaction, offsetInGroup);
}

void NodeGroup::flush(BMFileHandle& dataFH) {
    if (chunkedGroups.getNumChunkedGroups() == 0) {
        return;
    }
    if (chunkedGroups.getNumChunkedGroups() == 1) {
        auto flushedChunkGroup = chunkedGroups.getChunkedGroup(0).flush(dataFH);
        chunkedGroups.setChunkedGroup(0, std::move(flushedChunkGroup));
    } else {
        // Merge all chunkedGroups into a single one first. Then flush it to disk.
        const auto mergedChunkedGroup = std::make_unique<ChunkedNodeGroup>(dataTypes,
            enableCompression, StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
        for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
            mergedChunkedGroup->append(*chunkedGroup, 0, chunkedGroup->getNumRows());
        }
        auto flushedChunkGroup = mergedChunkedGroup->flush(dataFH);
        chunkedGroups.setChunkedGroup(0, std::move(flushedChunkGroup));
    }
    setToOnDisk();
}

void NodeGroup::checkpoint(TableData& tableData, BMFileHandle& dataFH) {
    // We don't need to consider deletions here, as they are flushed separately as metadata.
    // A special but rare case can be all rows are deleted, then we can skip flushing the data.
    auto updateChunkedGroup = scanCommitted<ResidencyState::ON_DISK>(tableData);
    auto insertChunkedGroup = scanCommitted<ResidencyState::IN_MEMORY>(tableData);
    // if (residencyState == ResidencyState::IN_MEMORY) {
    // KU_ASSERT(updateChunkedGroup->getNumRows() == 0);
    // auto flushedChunkedGroup = insertChunkedGroup->flush(dataFH);
    // chunkedGroups.setChunkedGroup(0, std::move(flushedChunkedGroup));
    // setToOnDisk();
    // return;
    // }
    std::vector<std::unique_ptr<ColumnChunk>> checkpointedChunks;
    checkpointedChunks.resize(tableData.getNumColumns());
    const auto dummyTransaction = std::make_unique<Transaction>(TransactionType::READ_ONLY, 0,
        Transaction::START_TRANSACTION_ID - 1);
    auto scanState = createScanState(dummyTransaction.get(), tableData);
    for (auto columnID = 0u; columnID < tableData.getNumColumns(); columnID++) {
        checkpointedChunks[columnID] = tableData.getColumn(columnID)->checkpointColumnChunk(
            scanState.chunkStates[columnID], insertChunkedGroup->getColumnChunk(columnID),
            updateChunkedGroup->getColumnChunk(columnID));
    }
    auto checkpointedChunkedGroup =
        std::make_unique<ChunkedNodeGroup>(std::move(checkpointedChunks), startNodeOffset);
    chunkedGroups.setChunkedGroup(0, std::move(checkpointedChunkedGroup));
    setToOnDisk();
}

void NodeGroup::populateNodeID(ValueVector& nodeIDVector, SelectionVector& selVector,
    table_id_t tableID, const offset_t startNodeOffset, const row_idx_t numRows) {
    for (auto i = 0u; i < numRows; i++) {
        nodeIDVector.setValue<nodeID_t>(i, {startNodeOffset + i, tableID});
    }
    if (selVector.getSelSize() == numRows) {
        nodeIDVector.state->getSelVectorUnsafe().setToUnfiltered(numRows);
    } else {
        std::memcpy(nodeIDVector.state->getSelVectorUnsafe().getMultableBuffer().data(),
            selVector.getMultableBuffer().data(), selVector.getSelSize() * sizeof(sel_t));
        nodeIDVector.state->getSelVectorUnsafe().setToFiltered(selVector.getSelSize());
    }
}

bool NodeGroup::hasChanges() const {
    if (residencyState == ResidencyState::IN_MEMORY) {
        return true;
    }
    if (versionInfo.hasDeletions() || versionInfo.hasInsertions()) {
        return true;
    }
    for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        if (chunkedGroup->hasUpdates()) {
            return true;
        }
    }
    return false;
}

bool NodeGroup::hasInsertionsOrDeletions() const {
    return versionInfo.hasDeletions() || versionInfo.hasInsertions();
}

uint64_t NodeGroup::getEstimatedMemoryUsage() const {
    uint64_t memUsage = 0;
    for (const auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        memUsage += chunkedGroup->getEstimatedMemoryUsage();
    }
    return memUsage;
}

void NodeGroup::serialize(Serializer& serializer) const {
    KU_ASSERT(chunkedGroups.getNumChunkedGroups() == 1);
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    // Serialize nodeGroupIdx and metadata of chunks.
    serializer.write<node_group_idx_t>(nodeGroupIdx);
    serializer.write<bool>(enableCompression);
    chunkedGroups.getChunkedGroup(0).serialize(serializer);
    // Serialize deleted version informaton if any.
    const auto hasDeletions = versionInfo.hasDeletions();
    serializer.write<bool>(hasDeletions);
    if (hasDeletions) {
        versionInfo.serialize(serializer);
    }
}

std::unique_ptr<NodeGroup> NodeGroup::deserialize(Deserializer& deSer) {
    node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    deSer.deserializeValue<node_group_idx_t>(nodeGroupIdx);
    deSer.deserializeValue<bool>(enableCompression);
    auto chunkedNodeGroup = ChunkedNodeGroup::deserialize(deSer);
    bool hasDeletions;
    deSer.deserializeValue<bool>(hasDeletions);
    std::unique_ptr<NodeGroupVersionInfo> versionInfo = nullptr;
    if (hasDeletions) {
        versionInfo = NodeGroupVersionInfo::deserialize(deSer);
    }
    return std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression, std::move(chunkedNodeGroup),
        std::move(versionInfo));
}

std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanInMemCommitted() {
    auto mergedInMemGroup = std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
        StorageConstants::NODE_GROUP_SIZE, startNodeOffset, ResidencyState::IN_MEMORY);
    for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        if (chunkedGroup->getResidencyState() != ResidencyState::IN_MEMORY) {
            continue;
        }
        auto scannedChunkGroup = chunkedGroup->scanInMemCommitted();
        mergedInMemGroup->append(*scannedChunkGroup, 0, scannedChunkGroup->getNumRows());
    }
    return mergedInMemGroup;
}

template<ResidencyState SCAN_RESIDENCY_STATE>
std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted(TableData& tableData) {
    // TODO(Guodong): capacity should be set based on numValues.
    auto mergedInMemGroup = std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression,
        StorageConstants::NODE_GROUP_SIZE, startNodeOffset, ResidencyState::TEMPORARY);
    const auto dummyTransaction = std::make_unique<Transaction>(TransactionType::READ_ONLY, 0,
        Transaction::START_TRANSACTION_ID - 1);
    auto scanState = createScanState(dummyTransaction.get(), tableData);
    for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        chunkedGroup->scanCommitted<SCAN_RESIDENCY_STATE>(dummyTransaction.get(), scanState,
            *mergedInMemGroup);
    } // TODO: mergedInMemGroup is not set numRows.
    return mergedInMemGroup;
}

template std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted<ResidencyState::ON_DISK>(
    TableData& tableData);
template std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted<ResidencyState::IN_MEMORY>(
    TableData& tableData);
template std::unique_ptr<ChunkedNodeGroup> NodeGroup::scanCommitted<ResidencyState::TEMPORARY>(
    TableData& tableData);

TableScanState NodeGroup::createScanState(Transaction* transaction, TableData& tableData) const {
    std::vector<column_id_t> columnIDsToScan;
    for (auto columnID = 0u; columnID < tableData.getNumColumns(); columnID++) {
        columnIDsToScan.push_back(columnID);
    }
    TableScanState scanState(INVALID_TABLE_ID, columnIDsToScan);
    scanState.tableData = &tableData;
    initializeScanState(transaction, scanState);
    return scanState;
}

} // namespace storage
} // namespace kuzu
