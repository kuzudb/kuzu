#include "storage/index/hnsw_graph.h"

#include <iostream>

#include "storage/store/list_chunk_data.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {

InMemEmbeddings::InMemEmbeddings(MemoryManager& mm, EmbeddingTypeInfo typeInfo,
    common::offset_t numNodes, const common::LogicalType& columnType)
    : EmbeddingColumn{std::move(typeInfo)} {
    auto numNodeGroups = StorageUtils::getNodeGroupIdx(numNodes) + 1;
    data.resize(numNodeGroups);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto numNodesInGroup = std::min(common::StorageConfig::NODE_GROUP_SIZE,
            numNodes - i * common::StorageConfig::NODE_GROUP_SIZE);
        data[i] = ColumnChunkFactory::createColumnChunkData(mm, columnType.copy(),
            false /*enableCompression*/, numNodesInGroup, ResidencyState::IN_MEMORY,
            true /*hasNullData*/, false /*initializeToZero*/);
        data[i]->cast<ListChunkData>().getDataColumnChunk()->resize(
            numNodesInGroup * this->typeInfo.dimension);
    }
}

float* InMemEmbeddings::getEmbedding(common::offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    KU_ASSERT(nodeGroupIdx < data.size());
    const auto& listChunk = data[nodeGroupIdx]->cast<ListChunkData>();
    return &listChunk.getDataColumnChunk()->getData<float>()[offsetInGroup * typeInfo.dimension];
}

void InMemEmbeddings::initialize(main::ClientContext* context, NodeTable& table,
    common::column_id_t columnID) {
    common::TimeMetric timeMetric(true);
    timeMetric.start();
    std::vector<common::column_id_t> columnIDs;
    columnIDs.push_back(columnID);
    std::vector<const Column*> columns;
    auto& column = table.getColumn(columnID);
    columns.push_back(&column);
    const auto scanState =
        std::make_unique<NodeTableScanState>(table.getTableID(), columnIDs, columns);
    scanState->source = TableScanSource::COMMITTED;
    for (auto i = 0u; i < data.size(); i++) {
        scanState->nodeGroupIdx = i;
        column.scan(context->getTransaction(), scanState->nodeGroupScanState->chunkStates[0],
            data[i].get());
    }
    timeMetric.stop();
    auto duration = timeMetric.getElapsedTimeMS();
    std::cout << "Initialize embeddings: " << duration << " ms." << std::endl;
    uint64_t memUsageInMB = 0;
    for (auto i = 0u; i < data.size(); i++) {
        memUsageInMB += data[i]->getEstimatedMemoryUsage() * 1.0 / 1024.0 / 1024.0;
    }
    std::cout << "Embeddings mem usage: " << memUsageInMB << " MB." << std::endl;
}

OnDiskEmbeddingScanState::OnDiskEmbeddingScanState(MemoryManager* mm, NodeTable& nodeTable,
    common::column_id_t columnID) {
    std::vector<common::column_id_t> columnIDs;
    columnIDs.push_back(columnID);
    std::vector<const Column*> columns;
    columns.push_back(&nodeTable.getColumn(columnID));
    std::vector<common::LogicalType> types;
    types.push_back(columns.back()->getDataType().copy());
    propertyVectors = Table::constructDataChunk(mm, std::move(types));
    nodeIDVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID(), mm,
        propertyVectors.state);
    scanState = std::make_unique<NodeTableScanState>(nodeTable.getTableID(), std::move(columnIDs),
        std::move(columns), propertyVectors, nodeIDVector.get());
}

OnDiskEmbeddings::OnDiskEmbeddings(EmbeddingTypeInfo typeInfo, NodeTable& nodeTable,
    common::column_id_t columnID)
    : EmbeddingColumn{std::move(typeInfo)}, nodeTable{nodeTable}, columnID{columnID} {}

float* OnDiskEmbeddings::getEmbedding(transaction::Transaction* transaction,
    NodeTableScanState& scanState, common::offset_t offset) const {
    scanState.nodeIDVector->setValue(0, common::internalID_t{offset, nodeTable.getTableID()});
    scanState.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(1);
    scanState.source = TableScanSource::COMMITTED;
    scanState.nodeGroupIdx = StorageUtils::getNodeGroupIdx(offset);
    nodeTable.initScanState(transaction, scanState);
    const auto result = nodeTable.lookup(transaction, scanState);
    KU_ASSERT(result);
    KU_UNUSED(result);
    KU_ASSERT(scanState.outputVectors.size() == 1 &&
              scanState.outputVectors[0]->state->getSelVector()[0] == 0);
    const auto value = scanState.outputVectors[0]->getValue<common::list_entry_t>(0);
    KU_ASSERT(value.size == typeInfo.dimension);
    KU_UNUSED(value);
    const auto dataVector = common::ListVector::getDataVector(scanState.outputVectors[0]);
    return reinterpret_cast<float*>(dataVector->getData()) + value.offset;
}

void InMemHNSWGraph::finalize(MemoryManager& mm,
    const processor::PartitionerSharedState& partitionerSharedState) {
    auto bufferMemUsage = csrLengthBuffer->getBuffer().size() + dstNodesBuffer->getBuffer().size();
    auto bufferMemUsageInMB = bufferMemUsage * 1.0 / 1024.0 / 1024.0;
    std::cout << "CSR buffer mem usage " << bufferMemUsageInMB << " MB." << std::endl;
    common::TimeMetric timeMetric(true);
    timeMetric.start();
    const auto& partitionBuffers = partitionerSharedState.partitioningBuffers[0]->partitions;
    const auto numNodeGroups = (numNodes + common::StorageConfig::NODE_GROUP_SIZE - 1) /
                               common::StorageConfig::NODE_GROUP_SIZE;
    KU_ASSERT(numNodeGroups == partitionerSharedState.numPartitions[0]);
    numRelsPerNodeGroup.resize(numNodeGroups);
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        auto numRels = 0u;
        const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        const auto numNodesInGroup =
            std::min(common::StorageConfig::NODE_GROUP_SIZE, numNodes - startNodeOffset);
        for (auto i = 0u; i < numNodesInGroup; i++) {
            numRels += getCSRLength(startNodeOffset + i);
        }
        numRelsPerNodeGroup[nodeGroupIdx] = numRels;
    }
    uint64_t memUsage = 0u;
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        finalizeNodeGroup(mm, nodeGroupIdx, partitionerSharedState.srcNodeTable->getTableID(),
            partitionerSharedState.dstNodeTable->getTableID(),
            partitionerSharedState.relTable->getTableID(), *partitionBuffers[nodeGroupIdx]);
        memUsage += partitionBuffers[nodeGroupIdx]->getChunkedGroup(0).getEstimatedMemoryUsage();
    }
    timeMetric.stop();
    auto duration = timeMetric.getElapsedTimeMS();
    std::cout << "Finalize HNSW graph: " << duration << " ms." << std::endl;
    auto memUsageInMB = memUsage * 1.0 / 1024.0 / 1024.0;
    std::cout << "Finalized partitioning buffers memory usage: " << memUsageInMB << " MB."
              << std::endl;
}

void InMemHNSWGraph::finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
    common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto numNodesInGroup =
        std::min(common::StorageConfig::NODE_GROUP_SIZE, numNodes - startNodeOffset);
    // BOUND_ID, NBR_ID, REL_ID.
    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    auto numRelsInGroup = numRelsPerNodeGroup[nodeGroupIdx];
    auto chunkedNodeGroup =
        std::make_unique<ChunkedNodeGroup>(mm, columnTypes, false /* enableCompression */,
            numRelsInGroup, 0 /* startRowIdx */, ResidencyState::IN_MEMORY);

    auto currNumRels = 0u;
    auto& boundColumnChunk = chunkedNodeGroup->getColumnChunk(0).getData();
    auto& nbrColumnChunk = chunkedNodeGroup->getColumnChunk(1).getData();
    auto& relIDColumnChunk = chunkedNodeGroup->getColumnChunk(2).getData();
    boundColumnChunk.cast<InternalIDChunkData>().setTableID(srcNodeTableID);
    nbrColumnChunk.cast<InternalIDChunkData>().setTableID(dstNodeTableID);
    relIDColumnChunk.cast<InternalIDChunkData>().setTableID(relTableID);
    for (auto i = 0u; i < numNodesInGroup; i++) {
        const auto currNodeOffset = startNodeOffset + i;
        const auto csrLen = getCSRLength(currNodeOffset);
        const auto csrOffset = currNodeOffset * maxDegree;
        for (auto j = 0u; j < csrLen; j++) {
            boundColumnChunk.setValue<common::offset_t>(currNodeOffset, currNumRels);
            relIDColumnChunk.setValue<common::offset_t>(currNumRels, currNumRels);
            const auto nbrOffset = getDstNode(csrOffset + j);
            KU_ASSERT(nbrOffset < numNodes);
            nbrColumnChunk.setValue<common::offset_t>(nbrOffset, currNumRels);
            currNumRels++;
        }
    }
    chunkedNodeGroup->setNumRows(currNumRels);

    for (auto i = 0u; i < nbrColumnChunk.getNumValues(); i++) {
        const auto offset = nbrColumnChunk.getValue<common::offset_t>(i);
        KU_ASSERT(offset < numNodes);
        KU_UNUSED(offset);
    }

    partition.merge(std::move(chunkedNodeGroup));
}

common::offset_vec_t InMemHNSWGraph::getNeighbors(transaction::Transaction* transaction,
    common::offset_t nodeOffset) const {
    const auto numNbrs = getCSRLength(nodeOffset);
    return getNeighbors(transaction, nodeOffset, numNbrs);
}

common::offset_vec_t InMemHNSWGraph::getNeighbors(transaction::Transaction*,
    common::offset_t nodeOffset, common::length_t numNbrs) const {
    common::offset_vec_t neighbors;
    neighbors.reserve(numNbrs);
    for (common::offset_t i = 0; i < numNbrs; i++) {
        auto nbr = getDstNode(nodeOffset * maxDegree + i);
        // Note: we might have INVALID_OFFSET at the end of the array of neighbor nodes. This is due
        // to that when we append a new neighbor node to node x, we don't exclusively lock the x,
        // instead, we increment the csrLength first, then set the dstNode. This design eases lock
        // contentions. However, if this function (`getNeighbors`) is called before the dstNode is
        // set, we will get INVALID_OFFSET. As csrLength is always synchorized, this design
        // shouldn't have correctness issue.
        if (nbr == common::INVALID_OFFSET) {
            continue;
        }
        neighbors.push_back(nbr);
    }
    return neighbors;
}

void InMemHNSWGraph::resetCSRLengthAndDstNodes() {
    for (common::offset_t i = 0; i < numNodes; i++) {
        setCSRLength(i, 0);
    }
    for (common::offset_t i = 0; i < numNodes * maxDegree; i++) {
        setDstNode(i, common::INVALID_OFFSET);
    }
}

} // namespace storage
} // namespace kuzu
