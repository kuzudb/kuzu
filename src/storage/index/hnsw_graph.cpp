#include "storage/index/hnsw_graph.h"

#include "storage/index/hnsw_index_utils.h"
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

float* InMemEmbeddings::getEmebdding(common::offset_t offset, transaction::Transaction*) const {
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
    // TODO: Refactor how we init scanState object.
    common::DataChunk dataChunk(2, chunkState);
    dataChunk.insert(0, std::make_shared<common::ValueVector>(common::LogicalType::INTERNAL_ID()));
    dataChunk.insert(1, std::make_shared<common::ValueVector>(columns[0]->getDataType().copy()));
    scanState->nodeIDVector = &dataChunk.getValueVectorMutable(0);
    scanState->outputVectors.push_back(&dataChunk.getValueVectorMutable(1));
    scanState->rowIdxVector->state = chunkState;
    scanState->outState = chunkState.get();
    // Scan from base table. Copy into embeddings.
    // Note that we omit transaction local data here.
    const auto numCommittedNodeGroups = table.getNumCommittedNodeGroups();
    scanState->source = TableScanSource::COMMITTED;
    for (auto i = 0u; i < numCommittedNodeGroups; i++) {
        scanState->nodeGroupIdx = i;
        table.initScanState(context->getTx(), *scanState);
        while (table.scan(context->getTx(), *scanState)) {
            data->append(scanState->outputVectors[0], scanState->outState->getSelVector());
        }
    }
}

OnDiskEmbeddings::OnDiskEmbeddings(MemoryManager* mm, EmbeddingTypeInfo typeInfo,
    NodeTable& nodeTable, common::column_id_t columnID)
    : EmbeddingColumn{std::move(typeInfo)}, nodeTable{nodeTable}, columnID{columnID} {
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

float* OnDiskEmbeddings::getEmebdding(common::offset_t offset,
    transaction::Transaction* transaction) const {
    scanState->nodeIDVector->setValue(0, common::internalID_t{offset, nodeTable.getTableID()});
    scanState->nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(1);
    scanState->source = TableScanSource::COMMITTED;
    scanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(offset);
    nodeTable.initScanState(transaction, *scanState);
    const auto result = nodeTable.lookup(transaction, *scanState);
    KU_ASSERT(result);
    KU_UNUSED(result);
    KU_ASSERT(scanState->outputVectors.size() == 1 &&
              scanState->outputVectors[0]->state->getSelVector()[0] == 0);
    const auto value = scanState->outputVectors[0]->getValue<common::list_entry_t>(0);
    KU_ASSERT(value.size == typeInfo.dimension);
    KU_UNUSED(value);
    const auto dataVector = common::ListVector::getDataVector(scanState->outputVectors[0]);
    return reinterpret_cast<float*>(dataVector->getData());
}

void InMemHNSWGraph::shrinkNbrs(common::offset_t offset, transaction::Transaction* transaction) {
    if (csrLengths[offset] <= maxDegree) {
        return;
    }
    shrinkToThreshold(offset, transaction);
}

void InMemHNSWGraph::shrinkToThreshold(common::offset_t offset,
    transaction::Transaction* transaction) {
    if (csrLengths[offset] <= shrinkThreshold) {
        return;
    }
    std::vector<NodeWithDistance> nbrs;
    const auto vector = embeddings->getEmebdding(offset, transaction);
    for (const auto nbr : getNeighbors(offset, transaction)) {
        const auto dist = HNSWIndexUtils::computeDistance(*embeddings, vector, nbr, transaction);
        nbrs.push_back({nbr, dist});
    }
    std::ranges::sort(nbrs, [](const NodeWithDistance& l, const NodeWithDistance& r) {
        return l.distance < r.distance;
    });
    uint64_t newSize = 0;
    for (auto i = 1u; i < nbrs.size(); i++) {
        bool keepNbr = true;
        for (auto j = i + 1; j < nbrs.size(); j++) {
            const auto nbrVector = embeddings->getEmebdding(nbrs[i].nodeOffset, transaction);
            const auto dist = HNSWIndexUtils::computeDistance(*embeddings, nbrVector,
                nbrs[j].nodeOffset, transaction);
            if (ALPHA * dist < nbrs[i].distance) {
                keepNbr = false;
                break;
            }
        }
        if (keepNbr) {
            dstNodes[newSize++] = nbrs[i].nodeOffset;
        }
        if (newSize == shrinkThreshold) {
            break;
        }
    }
    csrLengths[offset] = newSize;
}

void InMemHNSWGraph::finalize(MemoryManager& mm,
    processor::PartitionerSharedState& partitionerSharedState) {
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
            numRels += csrLengths.operator[](startNodeOffset + i);
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
        const auto csrOffset = i * maxDegree;
        const auto csrLen = csrLengths[startNodeOffset + i];
        const auto currNodeOffset = startNodeOffset + i;
        for (auto j = 0u; j < csrLen; j++) {
            boundColumnChunk.setValue<common::offset_t>(currNodeOffset, currNumRels);
            relIDColumnChunk.setValue<common::offset_t>(currNumRels, currNumRels);
            const auto nbrOffset = dstNodes.operator[](csrOffset + j);
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

void InMemHNSWGraph::createDirectedEdge(common::offset_t srcNode, common::offset_t dstNode,
    transaction::Transaction* transaction) {
    if (csrLengths[srcNode] >= maxDegree) {
        shrinkNbrs(srcNode, transaction);
    }
    dstNodes[srcNode * maxDegree + csrLengths[srcNode]] = dstNode;
    KU_ASSERT(dstNode < numNodes);
    csrLengths[srcNode]++;
}

common::offset_vec_t InMemHNSWGraph::getNeighbors(common::offset_t node,
    transaction::Transaction*) const {
    std::vector<common::offset_t> neighbors;
    neighbors.reserve(csrLengths[node]);
    for (common::offset_t i = 0; i < csrLengths[node]; i++) {
        neighbors.push_back(dstNodes[node * maxDegree + i]);
    }
    return neighbors;
}

void InMemHNSWGraph::resetCSRLengthAndDstNodes() {
    for (common::offset_t i = 0; i < numNodes; i++) {
        csrLengths[i] = 0;
    }
    for (common::offset_t i = 0; i < numNodes * maxDegree; i++) {
        dstNodes[i] = common::INVALID_OFFSET;
    }
}

OnDiskHNSWGraph::OnDiskHNSWGraph(main::ClientContext* context, common::length_t maxDegree,
    NodeTable& nodeTable, RelTable& relTable, EmbeddingColumn* embeddings)
    : HNSWGraph{nodeTable.getNumTotalRows(context->getTx()), maxDegree, embeddings},
      nodeTable{nodeTable}, relTable{relTable} {
    // TODO(Guodong): Only scan committed data here. We need to scan local data as well.
    auto columnIDs = std::vector{NBR_ID_COLUMN_ID};
    auto columns = std::vector<const Column*>();
    auto direction = common::RelDataDirection::FWD;
    columns.push_back(relTable.getColumn(NBR_ID_COLUMN_ID, direction));
    scanState = std::make_unique<RelTableScanState>(*context->getMemoryManager(),
        relTable.getTableID(), std::move(columnIDs), std::move(columns),
        relTable.getCSROffsetColumn(direction), relTable.getCSRLengthColumn(direction), direction);
    srcNodeIDVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID(),
        context->getMemoryManager());
    srcNodeIDVector->state = std::make_shared<common::DataChunkState>();
    dstNodeIDVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID(),
        context->getMemoryManager());
    dstNodeIDVector->state = std::make_shared<common::DataChunkState>();
    scanState->nodeIDVector = srcNodeIDVector.get();
    scanState->outputVectors.push_back(dstNodeIDVector.get());
    scanState->outState = dstNodeIDVector->state.get();
    scanState->rowIdxVector->state = dstNodeIDVector->state;
}

common::offset_vec_t OnDiskHNSWGraph::getNeighbors(common::offset_t offset,
    transaction::Transaction* transaction) const {
    scanState->nodeIDVector->setValue(0, common::internalID_t{offset, nodeTable.getTableID()});
    scanState->nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(1);
    scanState->outState->getSelVectorUnsafe().setSelSize(0);
    relTable.initScanState(transaction, *scanState);
    common::offset_vec_t result;
    while (true) {
        if (!relTable.scan(transaction, *scanState)) {
            break;
        }
        for (auto i = 0u; i < scanState->outState->getSelSize(); i++) {
            result.push_back(scanState->outputVectors[0]->getValue<common::internalID_t>(i).offset);
        }
    }
    return result;
}

} // namespace storage
} // namespace kuzu
