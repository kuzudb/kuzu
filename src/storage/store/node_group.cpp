#include "storage/store/node_group.h"

#include "storage/store/table.h"
#include <storage/store/node_table.h>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void NodeGroup::append(Transaction* transaction, const ChunkedNodeGroupCollection& chunkCollection,
    row_idx_t offset, row_idx_t numRowsToAppend) {
    const auto startOffset = chunkedGroups.append(chunkCollection, offset, numRowsToAppend);
    versionInfo.append(transaction->getID(), startOffset, numRowsToAppend);
}

void NodeGroup::append(Transaction* transaction, const ChunkedNodeGroup& chunkedGroup) {
    const auto startOffset = chunkedGroups.append(transaction, chunkedGroup);
    const auto numRows = chunkedGroup.getNumRows();
    versionInfo.append(transaction->getID(), startOffset, numRows);
}

void NodeGroup::append(Transaction* transaction, const std::vector<ValueVector*>& vectors,
    row_idx_t startRowIdx, row_idx_t numRowsToAppend) {
    // TODO: Remove the assumption of all vectors have the same selVector.
    auto& anchorSelVector = vectors[0]->state->getSelVector();
    SelectionVector selVector(DEFAULT_VECTOR_CAPACITY);
    auto selVectorBuffer = selVector.getMultableBuffer();
    for (auto i = 0u; i < numRowsToAppend; i++) {
        selVectorBuffer[i] = anchorSelVector[startRowIdx + i];
    }
    selVector.setToFiltered(numRowsToAppend);
    const auto startOffset = chunkedGroups.append(vectors, selVector);
    versionInfo.append(transaction->getID(), startOffset, numRowsToAppend);
}

void NodeGroup::merge(Transaction*, std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    chunkedGroups.merge(std::move(chunkedGroup));
}

void NodeGroup::initializeScanState(Transaction*, TableScanState& state) const {
    auto& nodeGroupState = state.nodeGroupScanState;
    // TODO: Should handle transaction to resolve version visibility here.
    nodeGroupState.maxNumRowsToScan = getNumRows();
    if (type == NodeGroupType::ON_DISK) {
        auto& nodeScanState = state.cast<NodeTableScanState>();
        KU_ASSERT(chunkedGroups.getNumChunkedGroups() == 1);
        auto& chunkedGroup = chunkedGroups.getChunkedGroup(0);
        for (auto columnIdx = 0u; columnIdx < state.columnIDs.size(); columnIdx++) {
            const auto columnID = state.columnIDs[columnIdx];
            auto& chunk = chunkedGroup.getColumnChunk(columnID);
            chunk.initializeScanState(nodeScanState.chunkStates[columnIdx]);
        }
    }
}

bool NodeGroup::scan(Transaction* transaction, TableScanState& state) const {
    // TODO: Should handle transaction to resolve version visibility here.
    KU_ASSERT(state.source == TableScanSource::COMMITTED);
    auto& nodeGroupState = state.nodeGroupScanState;
    KU_ASSERT(nodeGroupState.chunkedGroupIdx < chunkedGroups.getNumChunkedGroups());
    KU_ASSERT(nodeGroupState.nextRowToScan < nodeGroupState.maxNumRowsToScan);
    auto& chunkedGroup = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
    if (nodeGroupState.nextRowToScan ==
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
    if (selVector.getSelSize() == 0) {
        return true;
    }
    const auto nodeOffset = startNodeOffset + nodeGroupState.nextRowToScan;
    for (auto i = 0u; i < numRowsToScan; i++) {
        state.nodeIDVector->setValue<nodeID_t>(i, {nodeOffset + i, state.tableID});
    }
    if (selVector.getSelSize() == numRowsToScan) {
        state.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(numRowsToScan);
    } else {
        std::memcpy(state.nodeIDVector->state->getSelVectorUnsafe().getMultableBuffer().data(),
            selVector.getMultableBuffer().data(), selVector.getSelSize() * sizeof(sel_t));
        state.nodeIDVector->state->getSelVectorUnsafe().setToFiltered(selVector.getSelSize());
    }
    if (type == NodeGroupType::IN_MEMORY) {
        chunkedGroupToScan.scan(state.columnIDs, state.outputVectors, offsetToScan, numRowsToScan);
    } else {
        auto& nodeScanState = state.constCast<NodeTableScanState>();
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            nodeScanState.columns[i]->scan(transaction, nodeScanState.chunkStates[i], offsetToScan,
                numRowsToScan, nodeScanState.nodeIDVector, state.outputVectors[i]);
        }
    }
    nodeGroupState.nextRowToScan += numRowsToScan;
    return true;
}

void NodeGroup::lookup(Transaction* transaction, TableScanState& state) const {
    KU_ASSERT(state.nodeIDVector->state->getSelVector().getSelSize() == 1);
    if (type == NodeGroupType::IN_MEMORY) {
        const auto nodeOffset = state.nodeIDVector->getValue<nodeID_t>(0).offset;
        const auto offsetInGroup = nodeOffset - startNodeOffset;
        auto& chunkedGroupToScan = chunkedGroups.findChunkedGroupFromOffset(offsetInGroup);
        chunkedGroupToScan.lookup(state.columnIDs, state.outputVectors, offsetInGroup);
    } else {
        auto& nodeScanState = state.cast<NodeTableScanState>();
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            nodeScanState.columns[i]->lookup(transaction, nodeScanState.chunkStates[i],
                nodeScanState.nodeIDVector, state.outputVectors[i]);
        }
    }
}

void NodeGroup::update() {}

bool NodeGroup::delete_(Transaction* transaction, offset_t nodeOffset) {
    const auto offsetInGroup = nodeOffset - startNodeOffset;
    return versionInfo.delete_(transaction->getID(), offsetInGroup);
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
        // TODO: Should take `enableCompression` as a param from `NodeGroup`.
        const auto mergedChunkedGroup = std::make_unique<ChunkedNodeGroup>(dataTypes,
            true /*enableCompression*/, StorageConstants::NODE_GROUP_SIZE, 0);
        for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
            mergedChunkedGroup->append(*chunkedGroup, 0, chunkedGroup->getNumRows());
        }
        auto flushedChunkGroup = mergedChunkedGroup->flush(dataFH);
        chunkedGroups.setChunkedGroup(0, std::move(flushedChunkGroup));
    }
    setToOnDisk();
}

} // namespace storage
} // namespace kuzu
