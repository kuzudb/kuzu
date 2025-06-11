#include "index/hnsw_rel_batch_insert.h"

#include "index/hnsw_index.h"
#include "storage/storage_utils.h"
#include "storage/table/csr_chunked_node_group.h"

namespace kuzu {
namespace vector_extension {

struct HNSWRelBatchInsertExecutionState : processor::RelBatchInsertExecutionState {
    HNSWRelBatchInsertExecutionState(const NodeToHNSWGraphOffsetMap& selectionMap,
        common::offset_t startNodeOffset);
    common::offset_t startNodeOffset;
    common::offset_t startNodeInGraph;
    common::offset_t endNodeInGraph;

    const NodeToHNSWGraphOffsetMap& selectionMap;

    common::offset_t getBoundNodeOffsetInGroup(common::offset_t offsetInGraph) const {
        return selectionMap.graphToNodeOffset(offsetInGraph) - startNodeOffset;
    }
};

HNSWRelBatchInsertExecutionState::HNSWRelBatchInsertExecutionState(
    const NodeToHNSWGraphOffsetMap& selectionMap, common::offset_t startNodeOffset)
    : startNodeOffset(startNodeOffset), selectionMap(selectionMap) {
    const auto endNodeOffset = std::min(selectionMap.numNodesInTable,
        startNodeOffset + common::StorageConfig::NODE_GROUP_SIZE);
    startNodeInGraph = selectionMap.nodeToGraphOffset(startNodeOffset, false);
    endNodeInGraph = selectionMap.nodeToGraphOffset(endNodeOffset, false);
    KU_ASSERT(startNodeInGraph <= endNodeInGraph);
    KU_ASSERT(endNodeInGraph <= selectionMap.numNodesInTable);
}

std::unique_ptr<processor::RelBatchInsertExecutionState> HNSWRelBatchInsert::initExecutionState(
    const processor::RelBatchInsertInfo&, common::node_group_idx_t nodeGroupIdx) {
    const auto& layerSharedState = partitionerSharedState->cast<HNSWLayerPartitionerSharedState>();
    auto& selectionMap = *layerSharedState.graphSelectionMap;
    return std::make_unique<HNSWRelBatchInsertExecutionState>(selectionMap,
        storage::StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx));
}

void HNSWRelBatchInsert::populateCSRLengths(processor::RelBatchInsertExecutionState& executionState,
    storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
    const processor::RelBatchInsertInfo&) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    const auto& hnswExecutionState = executionState.constCast<HNSWRelBatchInsertExecutionState>();
    const auto& layerSharedState = partitionerSharedState->cast<HNSWLayerPartitionerSharedState>();
    const auto& graph = layerSharedState.layer->getGraph();
    const auto startNodeInGraph = hnswExecutionState.startNodeInGraph;
    const auto endNodeInGraph = hnswExecutionState.endNodeInGraph;
    const auto lengthData =
        reinterpret_cast<common::length_t*>(csrHeader.length->getData().getData());
    std::fill(lengthData, lengthData + numNodes, 0);
    for (common::offset_t graphOffset = startNodeInGraph; graphOffset < endNodeInGraph;
         ++graphOffset) {
        const auto nodeOffsetInGroup = hnswExecutionState.getBoundNodeOffsetInGroup(graphOffset);
        KU_ASSERT(nodeOffsetInGroup < numNodes);
        lengthData[nodeOffsetInGroup] = graph.getCSRLength(graphOffset);
    }
}

static common::offset_t getNumRelsInGraph(const InMemHNSWGraph& graph,
    common::offset_t startNodeInGraph, common::offset_t endNodeInGraph) {
    auto numRels = 0u;
    for (auto offsetInGraph = startNodeInGraph; offsetInGraph < endNodeInGraph; offsetInGraph++) {
        numRels += graph.getCSRLength(offsetInGraph);
    }
    return numRels;
}

void HNSWRelBatchInsert::writeToTable(processor::RelBatchInsertExecutionState& executionState,
    const storage::ChunkedCSRHeader& csrHeader,
    const processor::RelBatchInsertLocalState& localState,
    processor::BatchInsertSharedState& sharedState, const processor::RelBatchInsertInfo&) {
    const auto& hnswExecutionState = executionState.constCast<HNSWRelBatchInsertExecutionState>();
    const auto& layerSharedState = partitionerSharedState->cast<HNSWLayerPartitionerSharedState>();
    const auto& graph = layerSharedState.layer->getGraph();
    const auto& selectionMap = *layerSharedState.graphSelectionMap;

    const auto startNodeInGraph = hnswExecutionState.startNodeInGraph;
    const auto endNodeInGraph = hnswExecutionState.endNodeInGraph;

    const auto numRels = getNumRelsInGraph(graph, startNodeInGraph, endNodeInGraph);
    sharedState.incrementNumRows(numRels);
    const auto startRelID = sharedState.table->cast<storage::RelTable>().reserveRelOffsets(numRels);

    static constexpr auto neighbourOffsetColumn = 0;
    static constexpr auto rowIdxColumn = 1;
    auto& neighbourChunk = localState.chunkedGroup->getColumnChunk(neighbourOffsetColumn).getData();
    auto& relIDChunk = localState.chunkedGroup->getColumnChunk(rowIdxColumn).getData();
    auto numRelsWritten = 0;
    for (common::offset_t nodeInGraph = startNodeInGraph; nodeInGraph < endNodeInGraph;
         ++nodeInGraph) {
        const auto boundNodeOffsetInGroup =
            hnswExecutionState.getBoundNodeOffsetInGroup(nodeInGraph);
        const auto neighbours = graph.getNeighbors(nodeInGraph);
        const auto boundNodeCSROffset = csrHeader.getStartCSROffset(boundNodeOffsetInGroup);
        common::idx_t neighbourIdx = 0;
        // write neighbour offset + unique rel ID for each rel
        for (const auto neighbourGraphOffset : neighbours) {
            const auto neighbourNodeOffset = selectionMap.graphToNodeOffset(neighbourGraphOffset);
            const auto relRowIdx = boundNodeCSROffset + neighbourIdx;
            const auto relID = startRelID + numRelsWritten;
            neighbourChunk.setValue(neighbourNodeOffset, relRowIdx);
            relIDChunk.setValue(relID, relRowIdx);

            ++neighbourIdx;
            ++numRelsWritten;
        }
    }

    KU_ASSERT(relIDChunk.getNumValues() == neighbourChunk.getNumValues());
    localState.chunkedGroup->setNumRows(neighbourChunk.getNumValues());
}
} // namespace vector_extension
} // namespace kuzu
