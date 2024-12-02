#include "storage/index/hnsw_graph.h"

#include "storage/store/list_chunk_data.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {

InMemEmbeddings::InMemEmbeddings(MemoryManager& mm, EmbeddingTypeInfo typeInfo,
    common::offset_t numNodes, const common::LogicalType& columnType)
    : EmbeddingColumn{std::move(typeInfo)} {
    data = ColumnChunkFactory::createColumnChunkData(mm, columnType.copy(),
        false /*enableCompression*/, numNodes, ResidencyState::IN_MEMORY, true /*hasNullData*/,
        false /*initializeToZero*/);
}

float* InMemEmbeddings::getEmbedding(common::offset_t offset) const {
    const auto& listChunk = data->cast<ListChunkData>();
    return &listChunk.getDataColumnChunk()->getData<float>()[offset * typeInfo.dimension];
}

void InMemEmbeddings::initialize(main::ClientContext* context, NodeTable& table,
    common::column_id_t columnID) {
    // Prepare scan state.
    std::vector<common::column_id_t> columnIDs;
    columnIDs.push_back(columnID);
    std::vector<const Column*> columns;
    columns.push_back(&table.getColumn(columnID));
    const auto scanState =
        std::make_unique<NodeTableScanState>(table.getTableID(), columnIDs, columns);
    const auto chunkState = std::make_shared<common::DataChunkState>();
    common::DataChunk dataChunk(2, chunkState);
    dataChunk.insert(0, std::make_shared<common::ValueVector>(common::LogicalType::INTERNAL_ID()));
    dataChunk.insert(1, std::make_shared<common::ValueVector>(columns[0]->getDataType().copy()));
    scanState->nodeIDVector = &dataChunk.getValueVectorMutable(0);
    scanState->outputVectors.push_back(&dataChunk.getValueVectorMutable(1));
    scanState->rowIdxVector->state = chunkState;
    scanState->outState = chunkState.get();
    // Scan from base table. Copy into embeddings.
    const auto numCommittedNodeGroups = table.getNumCommittedNodeGroups();
    scanState->source = TableScanSource::COMMITTED;
    for (auto i = 0u; i < numCommittedNodeGroups; i++) {
        scanState->nodeGroupIdx = i;
        table.initScanState(context->getTransaction(), *scanState);
        while (table.scan(context->getTransaction(), *scanState)) {
            data->append(scanState->outputVectors[0], scanState->outState->getSelVector());
        }
    }
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
    const auto& partitionBuffers = partitionerSharedState.partitioningBuffers[0]->partitions;
    const auto numNodeGroups = (numNodes + common::StorageConstants::NODE_GROUP_SIZE - 1) /
                               common::StorageConstants::NODE_GROUP_SIZE;
    KU_ASSERT(numNodeGroups == partitionerSharedState.numPartitions[0]);
    numRelsPerNodeGroup.resize(numNodeGroups);
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        auto numRels = 0u;
        const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        const auto numNodesInGroup =
            std::min(common::StorageConstants::NODE_GROUP_SIZE, numNodes - startNodeOffset);
        for (auto i = 0u; i < numNodesInGroup; i++) {
            numRels += getCSRLength(startNodeOffset + i);
        }
        numRelsPerNodeGroup[nodeGroupIdx] = numRels;
    }
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        finalizeNodeGroup(mm, nodeGroupIdx, partitionerSharedState.srcNodeTable->getTableID(),
            partitionerSharedState.dstNodeTable->getTableID(),
            partitionerSharedState.relTable->getTableID(), *partitionBuffers[nodeGroupIdx]);
    }
}

void InMemHNSWGraph::finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
    common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto numNodesInGroup =
        std::min(common::StorageConstants::NODE_GROUP_SIZE, numNodes - startNodeOffset);
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
