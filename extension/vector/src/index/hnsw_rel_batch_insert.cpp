#include "index/hnsw_rel_batch_insert.h"

#include "index/hnsw_index.h"
#include "storage/storage_utils.h"
#include "storage/table/csr_chunked_node_group.h"

namespace kuzu {
namespace vector_extension {

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void HNSWIndexPartitionerSharedState::setTables(storage::NodeTable* nodeTable,
    storage::RelTable* upperRelTable, storage::RelTable* lowerRelTable) {
    lowerPartitionerSharedState->srcNodeTable = nodeTable;
    lowerPartitionerSharedState->dstNodeTable = nodeTable;
    lowerPartitionerSharedState->relTable = lowerRelTable;
    upperPartitionerSharedState->srcNodeTable = nodeTable;
    upperPartitionerSharedState->dstNodeTable = nodeTable;
    upperPartitionerSharedState->relTable = upperRelTable;
}

void HNSWLayerPartitionerSharedState::setGraph(std::unique_ptr<InMemHNSWGraph> newGraph,
    std::unique_ptr<NodeToHNSWGraphOffsetMap> selectionMap) {
    graph = std::move(newGraph);
    graphSelectionMap = std::move(selectionMap);
}

void HNSWLayerPartitionerSharedState::resetState(common::idx_t partitioningIdx) {
    PartitionerSharedState::resetState(partitioningIdx);
    graph.reset();
    graphSelectionMap.reset();
}

std::unique_ptr<processor::RelBatchInsertImpl> HNSWRelBatchInsert::copy() {
    return std::make_unique<HNSWRelBatchInsert>(*this);
}

struct HNSWRelBatchInsertExecutionState : processor::RelBatchInsertExecutionState {
    HNSWRelBatchInsertExecutionState(const InMemHNSWGraph& graph,
        const NodeToHNSWGraphOffsetMap& selectionMap, common::offset_t startNodeOffset)
        : startNodeOffset(startNodeOffset), graph(graph), selectionMap(selectionMap) {
        const auto endNodeOffset = std::min(selectionMap.numNodesInTable,
            startNodeOffset + common::StorageConfig::NODE_GROUP_SIZE);
        startNodeInGraph = selectionMap.nodeToGraphOffset(startNodeOffset, false);
        endNodeInGraph = selectionMap.nodeToGraphOffset(endNodeOffset, false);
        KU_ASSERT(startNodeInGraph <= endNodeInGraph);
        KU_ASSERT(endNodeInGraph <= selectionMap.numNodesInTable);
    }
    common::offset_t startNodeOffset;
    common::offset_t startNodeInGraph;
    common::offset_t endNodeInGraph;

    const InMemHNSWGraph& graph;
    const NodeToHNSWGraphOffsetMap& selectionMap;

    common::offset_t getBoundNodeOffsetInGroup(common::offset_t offsetInGraph) const {
        return selectionMap.graphToNodeOffset(offsetInGraph) - startNodeOffset;
    }
};

std::unique_ptr<processor::RelBatchInsertExecutionState> HNSWRelBatchInsert::initExecutionState(
    const processor::PartitionerSharedState& partitionerSharedState,
    const processor::RelBatchInsertInfo&, common::node_group_idx_t nodeGroupIdx) {
    const auto& graphSharedState =
        partitionerSharedState.constCast<HNSWLayerPartitionerSharedState>();
    return std::make_unique<HNSWRelBatchInsertExecutionState>(*graphSharedState.graph,
        *graphSharedState.graphSelectionMap,
        storage::StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx));
}

void HNSWRelBatchInsert::populateCSRLengths(processor::RelBatchInsertExecutionState& executionState,
    storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
    const processor::RelBatchInsertInfo&) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    const auto& hnswExecutionState = executionState.constCast<HNSWRelBatchInsertExecutionState>();
    const auto& graph = hnswExecutionState.graph;
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
    const auto& graph = hnswExecutionState.graph;
    const auto& selectionMap = hnswExecutionState.selectionMap;

    const auto startNodeInGraph = hnswExecutionState.startNodeInGraph;
    const auto endNodeInGraph = hnswExecutionState.endNodeInGraph;

    const auto numRels = getNumRelsInGraph(graph, startNodeInGraph, endNodeInGraph);
    sharedState.incrementNumRows(numRels);
    const auto startRelID = sharedState.table->cast<storage::RelTable>().reserveRelOffsets(numRels);

    // Write the rel data of the HNSW index directly to the local chunked group
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
