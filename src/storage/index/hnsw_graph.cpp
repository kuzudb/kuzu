#include "storage/index/hnsw_graph.h"

#include "storage/store/list_chunk_data.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

InMemEmbeddings::InMemEmbeddings(transaction::Transaction* transaction, EmbeddingTypeInfo typeInfo,
    common::table_id_t tableID, common::column_id_t columnID)
    : EmbeddingColumn{std::move(typeInfo)} {
    auto key = common::stringFormat("{}-{}", tableID, columnID);
    auto& cacheManager = transaction->getLocalCacheManager();
    if (cacheManager.contains(key)) {
        data = transaction->getLocalCacheManager().at(key).cast<CachedColumn>();
    } else {
        throw common::RuntimeException("Miss cached column, which should never happen.");
    }
}

float* InMemEmbeddings::getEmbedding(common::offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    KU_ASSERT(nodeGroupIdx < data->columnChunks.size());
    const auto& listChunk = data->columnChunks[nodeGroupIdx]->cast<ListChunkData>();
    return &listChunk.getDataColumnChunk()->getData<float>()[offsetInGroup * typeInfo.dimension];
}

OnDiskEmbeddingScanState::OnDiskEmbeddingScanState(const transaction::Transaction* transaction,
    MemoryManager* mm, NodeTable& nodeTable, common::column_id_t columnID) {
    std::vector<common::column_id_t> columnIDs;
    columnIDs.push_back(columnID);
    std::vector<const Column*> columns;
    columns.push_back(&nodeTable.getColumn(columnID));
    std::vector<common::LogicalType> types;
    types.push_back(columns.back()->getDataType().copy());
    propertyVectors = Table::constructDataChunk(mm, std::move(types));
    nodeIDVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID(), mm,
        propertyVectors.state);
    std::vector<common::ValueVector*> outVectors;
    for (auto i = 0u; i < propertyVectors.valueVectors.size(); i++) {
        outVectors.push_back(propertyVectors.valueVectors[i].get());
    }
    scanState =
        std::make_unique<NodeTableScanState>(nodeIDVector.get(), outVectors, propertyVectors.state);
    scanState->setToTable(transaction, &nodeTable, std::move(columnIDs));
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

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWGraph::finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    const processor::PartitionerSharedState& partitionerSharedState) {
    const auto& partitionBuffers = partitionerSharedState.partitioningBuffers[0]->partitions;
    auto numRels = 0u;
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto numNodesInGroup =
        std::min(common::StorageConfig::NODE_GROUP_SIZE, numNodes - startNodeOffset);
    for (auto i = 0u; i < numNodesInGroup; i++) {
        numRels += getCSRLength(startNodeOffset + i);
    }
    finalizeNodeGroup(mm, nodeGroupIdx, numRels, partitionerSharedState.srcNodeTable->getTableID(),
        partitionerSharedState.dstNodeTable->getTableID(),
        partitionerSharedState.relTable->getTableID(), *partitionBuffers[nodeGroupIdx]);
}

void InMemHNSWGraph::finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    uint64_t numRels, common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
    common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto numNodesInGroup =
        std::min(common::StorageConfig::NODE_GROUP_SIZE, numNodes - startNodeOffset);
    // BOUND_ID, NBR_ID, REL_ID.
    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    columnTypes.push_back(common::LogicalType::INTERNAL_ID());
    auto chunkedNodeGroup = std::make_unique<ChunkedNodeGroup>(mm, columnTypes,
        false /* enableCompression */, numRels, 0 /* startRowIdx */, ResidencyState::IN_MEMORY);

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
    chunkedNodeGroup->setUnused(mm);
    partition.merge(std::move(chunkedNodeGroup));
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
