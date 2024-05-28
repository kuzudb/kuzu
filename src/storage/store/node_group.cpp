#include "storage/store/node_group.h"

#include "storage/store/table.h"
#include <storage/store/node_table.h>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void NodeGroup::append(Transaction* transaction, const ChunkedNodeGroup& chunkedGroup) {
    chunkedGroups.append(transaction, chunkedGroup);
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

void NodeGroup::scan(Transaction* transaction, TableScanState& state) const {
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
        return;
    }
    auto& chunkedGroupToScan = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
    const auto offsetToScan =
        nodeGroupState.nextRowToScan - chunkedGroupToScan.getStartNodeOffset();
    KU_ASSERT(offsetToScan < chunkedGroupToScan.getNumRows());
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - offsetToScan, DEFAULT_VECTOR_CAPACITY);
    if (type == NodeGroupType::IN_MEMORY) {
        chunkedGroupToScan.scan(state.columnIDs, state.outputVectors, offsetToScan, numRowsToScan);
    } else {
        auto& nodeScanState = state.constCast<NodeTableScanState>();
        for (auto i = 0u; i < state.columnIDs.size(); i++) {
            nodeScanState.columns[i]->scan(transaction, nodeScanState.chunkStates[i], offsetToScan,
                numRowsToScan, nodeScanState.nodeIDVector, state.outputVectors[i]);
        }
    }
    const auto nodeOffset = startNodeOffset + nodeGroupState.nextRowToScan;
    for (auto i = 0u; i < numRowsToScan; i++) {
        state.nodeIDVector->setValue<nodeID_t>(i, {nodeOffset + i, state.tableID});
    }
    state.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(numRowsToScan);

    nodeGroupState.nextRowToScan += numRowsToScan;
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
