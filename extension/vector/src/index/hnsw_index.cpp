#include "index/hnsw_index.h"

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "index/hnsw_rel_batch_insert.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace vector_extension {

InMemHNSWLayer::InMemHNSWLayer(MemoryManager* mm, InMemHNSWLayerInfo info)
    : entryPoint{common::INVALID_OFFSET}, info{info} {
    graph = std::make_unique<InMemHNSWGraph>(mm, info.numNodes, info.degreeThresholdToShrink);
}

std::vector<EmbeddingHandle> InMemHNSWLayerInfo::getEmbeddings(
    std::span<const common::offset_t> offsetsInGraph, GetEmbeddingsScanState& scanState) const {
    std::vector<common::offset_t> offsets;
    offsets.reserve(offsetsInGraph.size());
    for (size_t i = 0; i < offsetsInGraph.size(); ++i) {
        auto& offsetInGraph = offsetsInGraph[i];
        KU_ASSERT(offsetInGraph < numNodes);
        offsets.push_back(offsetMap.graphToNodeOffset(offsetInGraph));
    }
    return embeddings->getEmbeddings(offsets, scanState);
}

void InMemHNSWLayer::insert(common::offset_t offset, common::offset_t entryPoint_,
    VisitedState& visited, GetEmbeddingsScanState& scanState) {
    if (entryPoint_ == common::INVALID_OFFSET) {
        const auto entryPointInCurrentLayer = compareAndSwapEntryPoint(offset);
        if (entryPointInCurrentLayer == common::INVALID_OFFSET) {
            // The layer is empty. No edges need to be created.
            return;
        }
        entryPoint_ = entryPointInCurrentLayer;
    }

    auto embedding = info.getEmbedding(offset, scanState);
    const auto closest =
        searchKNN(embedding, entryPoint_, info.maxDegree, info.efc, visited, scanState);

    for (const auto& n : closest) {
        insertRel(offset, n.nodeOffset, scanState);
        insertRel(n.nodeOffset, offset, scanState);
    }
}

static decltype(auto) getNodeOffsets(const compressed_offsets_t& nodes) {
    std::vector<common::offset_t> nbrOffsets;
    for (const auto nodeOffset : nodes) {
        if (nodeOffset == common::INVALID_OFFSET) {
            break;
        }
        nbrOffsets.push_back(nodeOffset);
    }
    return nbrOffsets;
}

common::offset_t InMemHNSWLayer::searchNN(const EmbeddingHandle& queryVector,
    common::offset_t entryNode, GetEmbeddingsScanState& scanState) const {
    auto currentNodeOffset = entryNode;
    if (entryNode == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();

    const auto currNodeVector = info.getEmbedding(currentNodeOffset, scanState);
    auto minDist =
        info.metricFunc(queryVector.getPtr(), currNodeVector.getPtr(), info.getDimension());

    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto nbrOffsets = getNodeOffsets(graph->getNeighbors(currentNodeOffset));
        auto nbrVectors = info.getEmbeddings(nbrOffsets, scanState);
        KU_ASSERT(nbrOffsets.size() == nbrVectors.size());
        for (common::offset_t i = 0; i < nbrOffsets.size(); ++i) {
            const auto nbrOffset = nbrOffsets[i];
            const auto* nbrVector = nbrVectors[i].getPtr();
            const auto dist = info.metricFunc(queryVector.getPtr(), nbrVector, info.getDimension());
            if (dist < minDist) {
                minDist = dist;
                currentNodeOffset = nbrOffset;
            }
        }
    }
    return currentNodeOffset;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::insertRel(common::offset_t srcNode, common::offset_t dstNode,
    GetEmbeddingsScanState& scanState) {
    const auto currentLen = graph->incrementCSRLength(srcNode);
    KU_ASSERT(srcNode < info.numNodes);
    graph->setDstNode(srcNode * info.degreeThresholdToShrink + currentLen, dstNode);
    if (currentLen == info.degreeThresholdToShrink - 1) {
        shrinkForNode(info, graph.get(), srcNode, currentLen, scanState);
    }
}

static void processEntryNodeInKNNSearch(const EmbeddingHandle& queryVector,
    const EmbeddingHandle& entryVector, common::offset_t entryNode, VisitedState& visited,
    const metric_func_t& metricFunc, uint64_t dimension, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& result) {
    auto dist = metricFunc(queryVector.getPtr(), entryVector.getPtr(), dimension);
    candidates.push({entryNode, dist});
    result.push({entryNode, dist});
    visited.add(entryNode);
}

static void processNbrNodeInKNNSearch(const EmbeddingHandle& queryVector,
    const EmbeddingHandle& nbrVector, common::offset_t nbrOffset, uint64_t ef,
    VisitedState& visited, const metric_func_t& metricFunc, uint64_t dimension,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& result) {
    visited.add(nbrOffset);
    if (nbrVector.isNull()) {
        return;
    }
    auto dist = metricFunc(queryVector.getPtr(), nbrVector.getPtr(), dimension);
    if (result.size() < ef || dist < result.top().distance) {
        if (result.size() >= ef) {
            result.pop();
        }
        result.push({nbrOffset, dist});
        candidates.push({nbrOffset, dist});
    }
}

std::vector<NodeWithDistance> InMemHNSWLayer::searchKNN(const EmbeddingHandle& queryVector,
    common::offset_t entryNode, common::length_t k, uint64_t configuredEf, VisitedState& visited,
    GetEmbeddingsScanState& scanState) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    visited.reset();

    const auto entryVector = info.getEmbedding(entryNode, scanState);
    processEntryNodeInKNNSearch(queryVector, entryVector, entryNode, visited, info.metricFunc,
        info.getDimension(), candidates, result);

    const auto ef = std::max(k, configuredEf);
    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (result.size() >= ef && candidateDist > result.top().distance) {
            break;
        }
        candidates.pop();
        auto nbrOffsets = getNodeOffsets(graph->getNeighbors(candidate));
        auto nbrVectors = info.getEmbeddings(nbrOffsets, scanState);
        for (common::offset_t i = 0; i < nbrOffsets.size(); ++i) {
            const auto nbrOffset = nbrOffsets[i];
            if (!visited.contains(nbrOffset)) {
                processNbrNodeInKNNSearch(queryVector, nbrVectors[i], nbrOffset, ef, visited,
                    info.metricFunc, info.getDimension(), candidates, result);
            }
        }
    }
    return HNSWIndex::popTopK(result, k);
}

struct NodeWithDistanceAndEmbedding {
    NodeWithDistanceAndEmbedding(common::offset_t nodeOffset, double_t dist,
        EmbeddingHandle embedding)
        : node(nodeOffset, dist), embedding(std::move(embedding)) {}
    NodeWithDistance node;
    EmbeddingHandle embedding;

    common::offset_t getNodeOffset() const { return node.nodeOffset; }
    double_t getDist() const { return node.distance; }
};

static std::vector<NodeWithDistanceAndEmbedding> populateNeighbours(const InMemHNSWLayerInfo& info,
    InMemHNSWGraph* graph, common::offset_t nodeOffset, common::length_t numNbrs,
    GetEmbeddingsScanState& scanState) {
    std::vector<NodeWithDistanceAndEmbedding> nbrs;
    const auto vector = info.getEmbedding(nodeOffset, scanState);
    const auto neighbors = graph->getNeighbors(nodeOffset);
    auto nbrOffsets = getNodeOffsets(neighbors);
    auto nbrVectors = info.getEmbeddings(nbrOffsets, scanState);
    nbrs.reserve(numNbrs);
    for (common::offset_t i = 0; i < nbrOffsets.size(); ++i) {
        const auto nbrOffset = nbrOffsets[i];
        auto& nbrVector = nbrVectors[i];
        const auto dist = info.metricFunc(vector.getPtr(), nbrVector.getPtr(), info.getDimension());
        nbrs.emplace_back(nbrOffset, dist, std::move(nbrVector));
    }
    return nbrs;
}

template<typename Embedding>
static bool checkEmbeddingValidity(const std::vector<NodeWithDistanceAndEmbedding>& nbrs,
    common::idx_t nbrIdx, Embedding& embeddings, GetEmbeddingsScanState& scanState) {
    const auto tmpEmbedding = embeddings.getEmbedding(nbrs[nbrIdx].getNodeOffset(), scanState);
    return std::memcmp(nbrs[nbrIdx].embedding.getPtr(), tmpEmbedding.getPtr(),
               embeddings.getDimension()) == 0;
}

void InMemHNSWLayer::shrinkForNode(const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph,
    common::offset_t nodeOffset, common::length_t numNbrs, GetEmbeddingsScanState& scanState) {
    auto nbrs = populateNeighbours(info, graph, nodeOffset, numNbrs, scanState);
    std::ranges::sort(nbrs,
        [](const NodeWithDistanceAndEmbedding& l, const NodeWithDistanceAndEmbedding& r) {
            return l.getDist() < r.getDist();
        });
    uint16_t newSize = 0;
    for (auto i = 1u; i < nbrs.size(); i++) {
        bool keepNbr = true;
        for (auto j = i + 1; j < nbrs.size(); j++) {
            KU_ASSERT(checkEmbeddingValidity(nbrs, i, info, scanState));
            KU_ASSERT(checkEmbeddingValidity(nbrs, j, info, scanState));
            const auto dist = info.metricFunc(nbrs[i].embedding.getPtr(),
                nbrs[j].embedding.getPtr(), info.getDimension());
            if (info.alpha * dist < nbrs[i].getDist()) {
                keepNbr = false;
                break;
            }
        }
        if (keepNbr) {
            const auto startCSROffset = nodeOffset * info.degreeThresholdToShrink;
            graph->setDstNode(startCSROffset + newSize++, nbrs[i].getNodeOffset());
        }
        if (newSize == info.maxDegree) {
            break;
        }
    }
    graph->setCSRLength(nodeOffset, newSize);
}

void InMemHNSWLayer::finalizeNodeGroup(common::node_group_idx_t nodeGroupIdx,
    common::offset_t numNodesInTable, const NodeToHNSWGraphOffsetMap& selectedNodesMap,
    GetEmbeddingsScanState& scanState) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto endNodeOffset =
        std::min(numNodesInTable, startNodeOffset + common::StorageConfig::NODE_GROUP_SIZE);
    const auto startNodeInGraph = selectedNodesMap.nodeToGraphOffset(startNodeOffset, false);
    const auto endNodeInGraph = selectedNodesMap.nodeToGraphOffset(endNodeOffset, false);
    for (auto offsetInGraph = startNodeInGraph; offsetInGraph < endNodeInGraph; offsetInGraph++) {
        KU_ASSERT(offsetInGraph < selectedNodesMap.getNumNodesInGraph() &&
                  selectedNodesMap.graphToNodeOffset(offsetInGraph) >= startNodeInGraph &&
                  selectedNodesMap.graphToNodeOffset(offsetInGraph) < endNodeOffset);
        const auto numNbrs = graph->getCSRLength(offsetInGraph);
        if (numNbrs <= info.maxDegree) {
            continue;
        }
        shrinkForNode(info, graph.get(), offsetInGraph, numNbrs, scanState);
    }
}

void InMemHNSWIndex::moveToPartitionState(HNSWIndexPartitionerSharedState& partitionState) {
    partitionState.lowerPartitionerSharedState->setGraph(lowerLayer->moveGraph(),
        std::move(lowerGraphSelectionMap));
    partitionState.upperPartitionerSharedState->setGraph(upperLayer->moveGraph(),
        std::move(upperGraphSelectionMap));
}

std::vector<NodeWithDistance> HNSWIndex::popTopK(max_node_priority_queue_t& result,
    common::length_t k) {
    // Gather top k elements from result priority queue.
    std::vector<NodeWithDistance> topK;
    topK.reserve(k);
    while (result.size() > k) {
        result.pop();
    }
    while (!result.empty() && topK.size() < k) {
        auto node = result.top();
        topK.push_back(node);
        result.pop();
    }
    std::ranges::reverse(topK);
    return topK;
}

int64_t HNSWIndex::getDegreeThresholdToShrink(int64_t degree) {
    return std::ceil(degree * DEFAULT_DEGREE_THRESHOLD_RATIO);
}

int64_t HNSWIndex::getMaximumSupportedMl() {
    return common::DEFAULT_VECTOR_CAPACITY / DEFAULT_DEGREE_THRESHOLD_RATIO;
}

static common::ArrayTypeInfo getArrayTypeInfo(NodeTable& table, common::column_id_t columnID) {
    const auto& columnType = table.getColumn(columnID).getDataType();
    KU_ASSERT(columnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto typeInfo = columnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    return common::ArrayTypeInfo{typeInfo->getChildType().copy(), typeInfo->getNumElements()};
}

static std::unique_ptr<HNSWIndexEmbeddings> constructEmbeddingsColumn(
    const main::ClientContext* context, const common::ArrayTypeInfo& typeInfo, NodeTable& table,
    common::column_id_t columnID, const HNSWIndexConfig& config) {
    if (config.cacheEmbeddingsColumn) {
        return std::make_unique<InMemEmbeddings>(context->getTransaction(),
            common::ArrayTypeInfo{typeInfo.getChildType().copy(), typeInfo.getNumElements()},
            table.getTableID(), columnID);
    } else {
        return std::make_unique<OnDiskEmbeddings>(context->getTransaction(),
            context->getMemoryManager(),
            common::ArrayTypeInfo{typeInfo.getChildType().copy(), typeInfo.getNumElements()}, table,
            columnID);
    }
}

static std::unique_ptr<common::NullMask> getUpperLayerSelectionMask(
    const HNSWIndexEmbeddings& embeddings, common::row_idx_t numNodes,
    const HNSWIndexConfig& config, common::RandomEngine& randomEngine) {
    auto getEmbeddingsScanState = embeddings.constructScanState();
    auto upperLayerSelectionMask = std::make_unique<common::NullMask>(numNodes);
    for (common::offset_t i = 0; i < numNodes; ++i) {
        const auto rand =
            randomEngine.nextRandomInteger(InMemHNSWIndex::INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
        if (rand <= InMemHNSWIndex::INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.pu) {
            const auto embedding = embeddings.getEmbedding(i, *getEmbeddingsScanState);
            if (!embedding.isNull()) {
                upperLayerSelectionMask->setNull(i, true);
            }
        }
    }
    return upperLayerSelectionMask;
}

InMemHNSWIndex::InMemHNSWIndex(const main::ClientContext* context, IndexInfo indexInfo,
    std::unique_ptr<IndexStorageInfo> storageInfo, NodeTable& table, common::column_id_t columnID,
    HNSWIndexConfig config)
    : HNSWIndex{std::move(indexInfo), std::move(storageInfo), std::move(config),
          getArrayTypeInfo(table, columnID)} {
    const auto numNodes = table.getNumTotalRows(context->getTransaction());
    embeddings = constructEmbeddingsColumn(context, typeInfo, table, columnID, this->config);
    upperLayerSelectionMask =
        getUpperLayerSelectionMask(*embeddings, numNodes, this->config, randomEngine);
    lowerGraphSelectionMap = std::make_unique<NodeToHNSWGraphOffsetMap>(numNodes);
    upperGraphSelectionMap =
        std::make_unique<NodeToHNSWGraphOffsetMap>(numNodes, upperLayerSelectionMask.get());
    lowerLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, embeddings.get(), this->metricFunc,
            getDegreeThresholdToShrink(this->config.ml), this->config.ml, this->config.alpha,
            this->config.efc, *lowerGraphSelectionMap});
    upperLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{upperLayerSelectionMask->countNulls(), embeddings.get(),
            this->metricFunc, getDegreeThresholdToShrink(this->config.mu), this->config.mu,
            this->config.alpha, this->config.efc, *upperGraphSelectionMap});
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
bool InMemHNSWIndex::insert(common::offset_t offset, CreateInMemHNSWLocalState* localState) {
    auto& scanState = *localState->embeddingsScanState;
    auto queryVector = embeddings->getEmbedding(offset, scanState);
    if (queryVector.isNull()) {
        return false;
    }
    const auto lowerEntryPoint = upperGraphOffsetToNodeOffset(
        upperLayer->searchNN(queryVector, upperLayer->getEntryPoint(), scanState));
    lowerLayer->insert(offset, lowerEntryPoint, localState->lowerVisited, scanState);
    if (upperLayerSelectionMask->isNull(offset)) {
        const auto offsetInUpperGraph = upperGraphSelectionMap->nodeToGraphOffset(offset);
        upperLayer->insert(offsetInUpperGraph, upperLayer->getEntryPoint(),
            localState->upperVisited, scanState);
    }
    return true;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::finalizeNodeGroup(common::node_group_idx_t nodeGroupIdx) {
    const auto numNodesInTable = lowerLayer->getNumNodes();
    auto scanState = embeddings->constructScanState();
    upperLayer->finalizeNodeGroup(nodeGroupIdx, numNodesInTable, *upperGraphSelectionMap,
        *scanState);
    lowerLayer->finalizeNodeGroup(nodeGroupIdx, numNodesInTable, *lowerGraphSelectionMap,
        *scanState);
}

std::shared_ptr<common::BufferWriter> HNSWStorageInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferWriter>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.write<common::table_id_t>(upperRelTableID);
    serializer.write<common::table_id_t>(lowerRelTableID);
    serializer.write<common::offset_t>(upperEntryPoint);
    serializer.write<common::offset_t>(lowerEntryPoint);
    serializer.write<common::offset_t>(numCheckpointedNodes);
    return bufferWriter;
}

std::unique_ptr<IndexStorageInfo> HNSWStorageInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::table_id_t upperRelTableID = common::INVALID_TABLE_ID;
    common::table_id_t lowerRelTableID = common::INVALID_TABLE_ID;
    common::offset_t upperEntryPoint = common::INVALID_OFFSET;
    common::offset_t lowerEntryPoint = common::INVALID_OFFSET;
    common::offset_t checkpointedNodeOffset = common::INVALID_OFFSET;
    common::Deserializer deSer{std::move(reader)};
    deSer.deserializeValue<common::table_id_t>(upperRelTableID);
    deSer.deserializeValue<common::table_id_t>(lowerRelTableID);
    deSer.deserializeValue<common::offset_t>(upperEntryPoint);
    deSer.deserializeValue<common::offset_t>(lowerEntryPoint);
    deSer.deserializeValue<common::offset_t>(checkpointedNodeOffset);
    return std::make_unique<HNSWStorageInfo>(upperRelTableID, lowerRelTableID, upperEntryPoint,
        lowerEntryPoint, checkpointedNodeOffset);
}

HNSWSearchState::HNSWSearchState(main::ClientContext* context,
    catalog::TableCatalogEntry* nodeTableEntry, catalog::TableCatalogEntry* upperRelTableEntry,
    catalog::TableCatalogEntry* lowerRelTableEntry, NodeTable& nodeTable,
    common::column_id_t columnID, common::offset_t numNodes, uint64_t k, QueryHNSWConfig config)
    : visited{numNodes},
      embeddings{std::make_unique<OnDiskEmbeddings>(context->getTransaction(),
          context->getMemoryManager(), getArrayTypeInfo(nodeTable, columnID), nodeTable, columnID)},
      embeddingScanState{context->getTransaction(), context->getMemoryManager(), nodeTable,
          columnID, embeddings->getDimension()},
      k{k}, config{config}, semiMask{nullptr}, upperRelTableEntry{upperRelTableEntry},
      lowerRelTableEntry{lowerRelTableEntry}, searchType{SearchType::UNFILTERED},
      nbrScanState{nullptr}, secondHopNbrScanState{nullptr} {
    ef = std::max(k, static_cast<uint64_t>(config.efs));
    graph::NativeGraphEntry lowerGraphEntry{{nodeTableEntry}, {lowerRelTableEntry}};
    lowerGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(lowerGraphEntry));
    graph::NativeGraphEntry upperGraphEntry{{nodeTableEntry}, {upperRelTableEntry}};
    upperGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(upperGraphEntry));
}

OnDiskHNSWIndex::HNSWInsertState::HNSWInsertState(main::ClientContext* context,
    catalog::TableCatalogEntry* nodeTableEntry, catalog::TableCatalogEntry* upperRelTableEntry,
    catalog::TableCatalogEntry* lowerRelTableEntry, NodeTable& nodeTable,
    common::column_id_t columnID, uint64_t degree)
    : searchState{context, nodeTableEntry, upperRelTableEntry, lowerRelTableEntry, nodeTable,
          columnID, nodeTable.getNumTotalRows(context->getTransaction()), degree,
          QueryHNSWConfig{}} {
    std::vector<common::LogicalType> insertTypes;
    insertTypes.push_back(common::LogicalType::INTERNAL_ID());
    insertTypes.push_back(common::LogicalType::INTERNAL_ID());
    insertChunk = Table::constructDataChunk(context->getMemoryManager(), std::move(insertTypes));
    insertChunk.state->getSelVectorUnsafe().setToUnfiltered(1);
    std::vector<common::LogicalType> srcNodeIDTypes;
    srcNodeIDTypes.push_back(common::LogicalType::INTERNAL_ID());
    srcNodeIDChunk =
        Table::constructDataChunk(context->getMemoryManager(), std::move(srcNodeIDTypes));
    srcNodeIDChunk.state->getSelVectorUnsafe().setToUnfiltered(1);
    std::vector dataVectors{&insertChunk.getValueVectorMutable(1)};
    relInsertState = std::make_unique<RelTableInsertState>(srcNodeIDChunk.getValueVectorMutable(0),
        insertChunk.getValueVectorMutable(0), std::move(dataVectors));
    relDeleteState = std::make_unique<RelTableDeleteState>(srcNodeIDChunk.getValueVectorMutable(0),
        insertChunk.getValueVectorMutable(0), insertChunk.getValueVectorMutable(1));
    relInsertState->logToWAL = false;
    relDeleteState->logToWAL = false;
}

OnDiskHNSWIndex::OnDiskHNSWIndex(const main::ClientContext* context, IndexInfo indexInfo,
    std::unique_ptr<IndexStorageInfo> storageInfo, HNSWIndexConfig config)
    : HNSWIndex{indexInfo, std::move(storageInfo), std::move(config),
          getArrayTypeInfo(
              context->getStorageManager()->getTable(indexInfo.tableID)->cast<NodeTable>(),
              indexInfo.columnIDs[0])},
      mm{context->getMemoryManager()},
      nodeTable{context->getStorageManager()->getTable(indexInfo.tableID)->cast<NodeTable>()} {
    KU_ASSERT(this->indexInfo.columnIDs.size() == 1);
    KU_ASSERT(nodeTable.getColumn(this->indexInfo.columnIDs[0]).getDataType().getLogicalTypeID() ==
              common::LogicalTypeID::ARRAY);
    const auto storageManager = context->getStorageManager();
    const auto& hnswStorageInfo = this->storageInfo->cast<HNSWStorageInfo>();
    lowerRelTable = storageManager->getTable(hnswStorageInfo.lowerRelTableID)->ptrCast<RelTable>();
    upperRelTable = storageManager->getTable(hnswStorageInfo.upperRelTableID)->ptrCast<RelTable>();
}

std::unique_ptr<Index> OnDiskHNSWIndex::load(main::ClientContext* context, StorageManager*,
    IndexInfo indexInfo, std::span<uint8_t> storageInfoBuffer) {
    auto reader =
        std::make_unique<common::BufferReader>(storageInfoBuffer.data(), storageInfoBuffer.size());
    auto storageInfo = HNSWStorageInfo::deserialize(std::move(reader));
    const auto catalog = context->getCatalog();
    const auto indexEntry =
        catalog->getIndex(context->getTransaction(), indexInfo.tableID, indexInfo.name);
    const auto auxInfo = indexEntry->getAuxInfo().cast<HNSWIndexAuxInfo>();
    return std::make_unique<OnDiskHNSWIndex>(context, std::move(indexInfo), std::move(storageInfo),
        auxInfo.config.copy());
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::search(Transaction* transaction,
    const EmbeddingHandle& queryVector, HNSWSearchState& searchState) const {
    auto result = searchFromCheckpointed(transaction, queryVector, searchState);
    searchFromUnCheckpointed(transaction, queryVector, searchState, result);
    result.resize(searchState.k);
    return result;
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchFromCheckpointed(Transaction* transaction,
    const EmbeddingHandle& queryVector, HNSWSearchState& searchState) const {
    auto entryPoint = searchNNInUpperLayer(queryVector, searchState);
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    if (entryPoint == common::INVALID_OFFSET) {
        if (hnswStorageInfo.lowerEntryPoint == common::INVALID_OFFSET) {
            // Both upper and lower layers are empty. Thus, the index is empty.
            return {};
        }
        entryPoint = hnswStorageInfo.lowerEntryPoint;
    }
    return searchKNNInLayer(transaction, queryVector, entryPoint, searchState, false);
}

void OnDiskHNSWIndex::searchFromUnCheckpointed(Transaction* transaction,
    const EmbeddingHandle& queryVector, HNSWSearchState& searchState,
    std::vector<NodeWithDistance>& result) const {
    const auto numTotalRows = nodeTable.getNumTotalRows(transaction);
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    const auto numUnCheckpointedTuples = numTotalRows - hnswStorageInfo.numCheckpointedNodes;
    if (numUnCheckpointedTuples == 0) {
        // If the index is fully checkpointed, we can skip brute force search.
        return;
    }
    result.reserve(numUnCheckpointedTuples + result.size());
    // TODO(Guodong): Perhaps should switch to scan instead of lookup here.
    for (auto offset = hnswStorageInfo.numCheckpointedNodes; offset < numTotalRows; offset++) {
        const auto vector =
            searchState.embeddings->getEmbedding(offset, searchState.embeddingScanState);
        if (vector.isNull()) {
            continue; // Skip null or deleted values.
        }
        auto dist = metricFunc(queryVector.getPtr(), vector.getPtr(),
            searchState.embeddings->getDimension());
        result.emplace_back(offset, dist);
    }
    std::ranges::sort(result, [](const NodeWithDistance& l, const NodeWithDistance& r) {
        return l.distance < r.distance;
    });
}

static std::tuple<catalog::TableCatalogEntry*, catalog::TableCatalogEntry*,
    catalog::TableCatalogEntry*>
getIndexTableCatalogEntries(const catalog::Catalog* catalog, const Transaction* transaction,
    const IndexInfo& indexInfo) {
    auto nodeTableEntry = catalog->getTableCatalogEntry(transaction, indexInfo.tableID);
    const auto upperRelTableName =
        HNSWIndexUtils::getUpperGraphTableName(nodeTableEntry->getTableID(), indexInfo.name);
    const auto lowerRelTableName =
        HNSWIndexUtils::getLowerGraphTableName(nodeTableEntry->getTableID(), indexInfo.name);
    auto& upperRelTableEntry = catalog->getTableCatalogEntry(transaction, upperRelTableName, true)
                                   ->cast<catalog::RelGroupCatalogEntry>();
    auto& lowerRelTableEntry = catalog->getTableCatalogEntry(transaction, lowerRelTableName, true)
                                   ->cast<catalog::RelGroupCatalogEntry>();
    return {nodeTableEntry, &upperRelTableEntry, &lowerRelTableEntry};
}

std::unique_ptr<Index::InsertState> OnDiskHNSWIndex::initInsertState(main::ClientContext* context,
    visible_func) {
    auto [nodeTableEntry, upperRelTableEntry, lowerRelTableEntry] =
        getIndexTableCatalogEntries(context->getCatalog(), context->getTransaction(), indexInfo);
    return std::make_unique<HNSWInsertState>(context, nodeTableEntry, upperRelTableEntry,
        lowerRelTableEntry, nodeTable, indexInfo.columnIDs[0], config.ml);
}

class CommitInsertEmbeddingScanState final : public GetEmbeddingsScanState {
public:
    explicit CommitInsertEmbeddingScanState(common::ValueVector* valueVector)
        : valueVector{valueVector} {}

    void* getEmbeddingPtr(const EmbeddingHandle& handle) override {
        KU_ASSERT(!handle.isNull());
        void* val = nullptr;
        const auto dataVector = common::ListVector::getDataVector(valueVector);
        common::TypeUtils::visit(
            dataVector->dataType,
            [&]<VectorElementType T>(
                T) { val = reinterpret_cast<T*>(dataVector->getData()) + handle.offsetInData; },
            [&](auto) { KU_UNREACHABLE; });
        KU_ASSERT(val != nullptr);
        return val;
    }
    void addEmbedding(const EmbeddingHandle&) override {
        // No-op, as we do not need to add embeddings in this context.
    }
    void reclaimEmbedding(const EmbeddingHandle&) override {
        // No-op, as we do not need to reclaim embeddings in this context.
    }

private:
    common::ValueVector* valueVector;
};

void OnDiskHNSWIndex::commitInsert(Transaction* transaction,
    const common::ValueVector& nodeIDVector, const std::vector<common::ValueVector*>& dataVectors,
    InsertState& insertState) {
    KU_ASSERT(dataVectors.size() == 1);
    KU_ASSERT(nodeIDVector.state->getSelSize() == dataVectors[0]->state->getSelSize());
    auto& hnswInsertState = insertState.cast<HNSWInsertState>();
    auto commitInsertScanState = std::make_unique<CommitInsertEmbeddingScanState>(dataVectors[0]);
    for (size_t i = 0; i < nodeIDVector.state->getSelSize(); ++i) {
        auto pos = nodeIDVector.state->getSelVector()[i];
        const auto offset = nodeIDVector.readNodeOffset(pos);
        auto valuePos = dataVectors[0]->state->getSelVector()[i];
        if (dataVectors[0]->isNull(valuePos)) {
            continue; // Skip null or deleted values.
        }
        EmbeddingHandle handle{dataVectors[0]->getValue<common::list_entry_t>(valuePos).offset,
            commitInsertScanState.get()};
        insertInternal(transaction, offset, handle, hnswInsertState);
        storageInfo->cast<HNSWStorageInfo>().numCheckpointedNodes = offset + 1;
    }
}

void OnDiskHNSWIndex::finalize(main::ClientContext* context) {
    auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    const auto numTotalRows = nodeTable.getNumTotalRows(&DUMMY_CHECKPOINT_TRANSACTION);
    if (numTotalRows == hnswStorageInfo.numCheckpointedNodes) {
        return;
    }
    auto [nodeTableEntry, upperRelTableEntry, lowerRelTableEntry] =
        getIndexTableCatalogEntries(context->getCatalog(), context->getTransaction(), indexInfo);
    const auto embeddingDim = typeInfo.constPtrCast<common::ArrayTypeInfo>()->getNumElements();
    const auto scanState = std::make_unique<OnDiskEmbeddingScanState>(context->getTransaction(), mm,
        nodeTable, indexInfo.columnIDs[0], embeddingDim);
    const auto insertState = std::make_unique<HNSWInsertState>(context, nodeTableEntry,
        upperRelTableEntry, lowerRelTableEntry, nodeTable, indexInfo.columnIDs[0], config.ml);
    // TODO(Guodong): Perhaps should switch to scan instead of lookup here.
    for (auto offset = hnswStorageInfo.numCheckpointedNodes; offset < numTotalRows; offset++) {
        const auto vector = insertState->searchState.embeddings->getEmbedding(offset, *scanState);
        if (vector.isNull()) {
            continue;
        }
        insertInternal(context->getTransaction(), offset, vector, *insertState);
    }
    for (const auto offset : insertState->upperNodesToShrink) {
        shrinkForNode(context->getTransaction(), offset, true, config.mu, *insertState);
    }
    for (const auto offset : insertState->lowerNodesToShrink) {
        shrinkForNode(context->getTransaction(), offset, false, config.ml, *insertState);
    }
    hnswStorageInfo.numCheckpointedNodes = numTotalRows;
}

void OnDiskHNSWIndex::checkpoint(main::ClientContext* context,
    storage::PageAllocator& pageAllocator) {
    auto [nodeTableEntry, upperRelTableEntry, lowerRelTableEntry] = getIndexTableCatalogEntries(
        context->getCatalog(), &DUMMY_CHECKPOINT_TRANSACTION, indexInfo);
    upperRelTable->checkpoint(context, upperRelTableEntry, pageAllocator);
    lowerRelTable->checkpoint(context, lowerRelTableEntry, pageAllocator);
}

void OnDiskHNSWIndex::insertInternal(Transaction* transaction, common::offset_t offset,
    const EmbeddingHandle& vector, HNSWInsertState& insertState) {
    // Search fow lower layer entry point.
    const auto entryPoint = searchNNInUpperLayer(vector, insertState.searchState);
    insertToLayer(transaction, offset, entryPoint, vector, insertState, false /*isUpperLayer*/);
    // Search the lower layer to insert new vector.
    const auto rand = randomEngine.nextRandomInteger(INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
    if (rand <= INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.pu) {
        const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
        insertToLayer(transaction, offset, hnswStorageInfo.upperEntryPoint, vector, insertState,
            true /*isUpperLayer*/);
    }
}

common::offset_t OnDiskHNSWIndex::searchNNInUpperLayer(const EmbeddingHandle& queryVector,
    HNSWSearchState& searchState) const {
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    auto currentNodeOffset = hnswStorageInfo.upperEntryPoint;
    if (currentNodeOffset == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    const auto& embeddings = searchState.embeddings;
    const auto currNodeVector =
        embeddings->getEmbedding(currentNodeOffset, searchState.embeddingScanState);
    double minDist = 0.0;
    if (!currNodeVector.isNull()) {
        minDist =
            metricFunc(queryVector.getPtr(), currNodeVector.getPtr(), embeddings->getDimension());
    }
    const auto scanState = searchState.upperGraph->prepareRelScan(*searchState.upperRelTableEntry,
        hnswStorageInfo.upperRelTableID, indexInfo.tableID, {} /* relProperties */);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto neighborItr = searchState.upperGraph->scanFwd(
            common::nodeID_t{currentNodeOffset, indexInfo.tableID}, *scanState);
        for (const auto neighborChunk : neighborItr) {
            neighborChunk.forEach([&](auto neighbors, auto, auto i) {
                auto neighbor = neighbors[i];
                const auto nbrVector =
                    embeddings->getEmbedding(neighbor.offset, searchState.embeddingScanState);
                if (!nbrVector.isNull()) {
                    const auto dist = metricFunc(queryVector.getPtr(), nbrVector.getPtr(),
                        embeddings->getDimension());
                    if (dist < minDist) {
                        minDist = dist;
                        currentNodeOffset = neighbor.offset;
                    }
                }
            });
        }
    }
    return currentNodeOffset;
}

void OnDiskHNSWIndex::initLayerSearchState(Transaction* transaction, HNSWSearchState& searchState,
    bool isUpperLayer) const {
    searchState.visited.reset();
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    const auto& hnswGraph = isUpperLayer ? searchState.upperGraph : searchState.lowerGraph;
    const auto relEntry =
        isUpperLayer ? searchState.upperRelTableEntry : searchState.lowerRelTableEntry;
    const auto relTableID =
        isUpperLayer ? hnswStorageInfo.upperRelTableID : hnswStorageInfo.lowerRelTableID;
    searchState.nbrScanState =
        hnswGraph->prepareRelScan(*relEntry, relTableID, indexInfo.tableID, {} /* relProperties */);
    searchState.searchType = getFilteredSearchType(transaction, searchState);
    if (searchState.searchType == SearchType::BLIND_TWO_HOP ||
        searchState.searchType == SearchType::DIRECTED_TWO_HOP) {
        searchState.secondHopNbrScanState = hnswGraph->prepareRelScan(*relEntry, relTableID,
            indexInfo.tableID, {} /* relProperties */);
    }
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchKNNInLayer(Transaction* transaction,
    const EmbeddingHandle& queryVector, common::offset_t entryNode, HNSWSearchState& searchState,
    bool isUpperLayer) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t results;
    initLayerSearchState(transaction, searchState, isUpperLayer);

    const auto entryVector =
        searchState.embeddings->getEmbedding(entryNode, searchState.embeddingScanState);
    if (!entryVector.isNull()) {
        auto dist = metricFunc(queryVector.getPtr(), entryVector.getPtr(),
            searchState.embeddings->getDimension());
        candidates.push({entryNode, dist});
        if (searchState.isMasked(entryNode)) {
            results.push({entryNode, dist});
        }
    } else {
        // This is to make sure in case the entry node is deleted, we can still continue the search.
        candidates.push({entryNode, std::numeric_limits<double_t>::max()});
    }
    searchState.visited.add(entryNode);
    initSearchCandidates(queryVector, searchState, candidates, results);

    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (results.size() >= searchState.ef && candidateDist > results.top().distance) {
            break;
        }
        candidates.pop();
        const auto& hnswGraph = isUpperLayer ? searchState.upperGraph : searchState.lowerGraph;
        auto neighborItr = hnswGraph->scanFwd(common::nodeID_t{candidate, indexInfo.tableID},
            *searchState.nbrScanState);
        switch (searchState.searchType) {
        case SearchType::UNFILTERED:
        case SearchType::ONE_HOP_FILTERED: {
            oneHopSearch(queryVector, neighborItr, searchState, candidates, results);
        } break;
        case SearchType::DIRECTED_TWO_HOP: {
            directedTwoHopFilteredSearch(queryVector, neighborItr, searchState, candidates,
                results);
        } break;
        case SearchType::BLIND_TWO_HOP: {
            blindTwoHopFilteredSearch(queryVector, neighborItr, searchState, candidates, results);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    return popTopK(results, searchState.k);
}

SearchType OnDiskHNSWIndex::getFilteredSearchType(Transaction* transaction,
    const HNSWSearchState& searchState) {
    if (!searchState.hasMask()) {
        return SearchType::UNFILTERED;
    }
    const auto selectivity = 1.0 * searchState.semiMask->getNumMaskedNodes() /
                             searchState.lowerGraph->getNumNodes(transaction);
    if (selectivity < searchState.config.blindSearchUpSelThreshold) {
        return SearchType::BLIND_TWO_HOP;
    }
    if (selectivity < searchState.config.directedSearchUpSelThreshold) {
        return SearchType::DIRECTED_TWO_HOP;
    }
    return SearchType::ONE_HOP_FILTERED;
}

void OnDiskHNSWIndex::initSearchCandidates(const EmbeddingHandle& queryVector,
    HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& results) const {
    switch (searchState.searchType) {
    case SearchType::ONE_HOP_FILTERED:
    case SearchType::DIRECTED_TWO_HOP:
    case SearchType::BLIND_TWO_HOP: {
        const auto initialCandidates = FILTERED_SEARCH_INITIAL_CANDIDATES - results.size();
        for (auto candidate : searchState.semiMask->collectMaskedNodes(initialCandidates)) {
            if (searchState.visited.contains(candidate)) {
                continue;
            }
            searchState.visited.add(candidate);
            const auto candidateVector =
                searchState.embeddings->getEmbedding(candidate, searchState.embeddingScanState);
            if (candidateVector.isNull()) {
                continue;
            }
            auto candidateDist = metricFunc(queryVector.getPtr(), candidateVector.getPtr(),
                searchState.embeddings->getDimension());
            candidates.push({candidate, candidateDist});
            results.push({candidate, candidateDist});
        }
    } break;
    default: {
        // DO NOTHING
    }
    }
}

void OnDiskHNSWIndex::oneHopSearch(const EmbeddingHandle& queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    for (const auto neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbors, auto, auto i) {
            const auto nbr = neighbors[i];
            if (!searchState.visited.contains(nbr.offset) && searchState.isMasked(nbr.offset)) {
                const auto nbrVector = searchState.embeddings->getEmbedding(nbr.offset,
                    searchState.embeddingScanState);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, searchState.ef,
                    searchState.visited, metricFunc, searchState.embeddings->getDimension(),
                    candidates, results);
            }
        });
    }
}

void OnDiskHNSWIndex::directedTwoHopFilteredSearch(const EmbeddingHandle& queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    int64_t numVisitedNbrs = 0;
    auto candidatesForSecHop = collectFirstHopNbrsDirected(queryVector, nbrItr, searchState,
        candidates, results, numVisitedNbrs);
    processSecondHopCandidates(queryVector, searchState, numVisitedNbrs, candidates, results,
        candidatesForSecHop);
}

min_node_priority_queue_t OnDiskHNSWIndex::collectFirstHopNbrsDirected(
    const EmbeddingHandle& queryVector, graph::Graph::EdgeIterator& nbrItr,
    HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& results, int64_t& numVisitedNbrs) const {
    min_node_priority_queue_t candidatesForSecHop;
    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbors, auto, auto i) {
            const auto neighbor = neighbors[i];
            auto nbrOffset = neighbor.offset;
            if (!searchState.visited.contains(nbrOffset)) {
                const auto nbrVector =
                    searchState.embeddings->getEmbedding(nbrOffset, searchState.embeddingScanState);
                if (!nbrVector.isNull()) {
                    auto dist = metricFunc(queryVector.getPtr(), nbrVector.getPtr(),
                        searchState.embeddings->getDimension());
                    candidatesForSecHop.push({nbrOffset, dist});
                    if (searchState.isMasked(nbrOffset)) {
                        if (results.size() < searchState.ef || dist < results.top().distance) {
                            if (results.size() == searchState.ef) {
                                results.pop();
                            }
                            numVisitedNbrs++;
                            searchState.visited.add(nbrOffset);
                            results.push({nbrOffset, dist});
                            candidates.push({nbrOffset, dist});
                        }
                    }
                }
            }
        });
    }
    return candidatesForSecHop;
}

void OnDiskHNSWIndex::blindTwoHopFilteredSearch(const EmbeddingHandle& queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    int64_t numVisitedNbrs = 0;
    const auto secondHopCandidates = collectFirstHopNbrsBlind(queryVector, nbrItr, searchState,
        candidates, results, numVisitedNbrs);
    processSecondHopCandidates(queryVector, searchState, numVisitedNbrs, candidates, results,
        secondHopCandidates);
}

common::offset_vec_t OnDiskHNSWIndex::collectFirstHopNbrsBlind(const EmbeddingHandle& queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
    int64_t& numVisitedNbrs) const {
    common::offset_vec_t secondHopCandidates;
    secondHopCandidates.reserve(config.ml);
    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbors, auto, auto i) {
            const auto nbr = neighbors[i];
            if (!searchState.visited.contains(nbr.offset)) {
                secondHopCandidates.push_back(nbr.offset);
                if (searchState.isMasked(nbr.offset)) {
                    numVisitedNbrs++;
                    auto nbrVector = searchState.embeddings->getEmbedding(nbr.offset,
                        searchState.embeddingScanState);
                    processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, searchState.ef,
                        searchState.visited, metricFunc, searchState.embeddings->getDimension(),
                        candidates, results);
                }
            }
        });
    }
    return secondHopCandidates;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void OnDiskHNSWIndex::insertToLayer(Transaction* transaction, common::offset_t offset,
    common::offset_t entryPoint, const EmbeddingHandle& queryVector, HNSWInsertState& insertState,
    bool isUpperLayer) {
    auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    if (entryPoint == common::INVALID_OFFSET) {
        auto& entryPointToSet =
            isUpperLayer ? hnswStorageInfo.upperEntryPoint : hnswStorageInfo.lowerEntryPoint;
        if (entryPointToSet == common::INVALID_OFFSET) {
            // The layer is empty. Set the entry point to the current offset.
            entryPointToSet = offset;
            return;
        }
        entryPoint = entryPointToSet;
    }
    const auto closest = searchKNNInLayer(transaction, queryVector, entryPoint,
        insertState.searchState, isUpperLayer);
    createRels(transaction, offset, closest, isUpperLayer, insertState);
    for (const auto& n : closest) {
        createRels(transaction, n.nodeOffset, {{offset, std::numeric_limits<double>::max()}},
            isUpperLayer, insertState);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void OnDiskHNSWIndex::createRels(Transaction* transaction, common::offset_t offset,
    const std::vector<NodeWithDistance>& nbrs, bool isUpperLayer, HNSWInsertState& insertState) {
    auto& relTable = isUpperLayer ? *upperRelTable : *lowerRelTable;
    const auto maxDegree = isUpperLayer ? config.mu : config.ml;
    // TODO(Guodong): Should add the optimization for batch insertions.
    for (const auto& n : nbrs) {
        if (n.nodeOffset == offset) {
            continue;
        }
        insertState.relInsertState->srcNodeIDVector.setValue(0,
            common::nodeID_t{offset, indexInfo.tableID});
        insertState.relInsertState->dstNodeIDVector.setValue(0,
            common::nodeID_t{n.nodeOffset, indexInfo.tableID});
        relTable.insert(transaction, *insertState.relInsertState);
    }
    const auto& searchState = insertState.searchState;
    const auto relTableEntry =
        isUpperLayer ? searchState.upperRelTableEntry : searchState.lowerRelTableEntry;
    auto& graph = isUpperLayer ? searchState.upperGraph : searchState.lowerGraph;
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    const auto relTableID =
        isUpperLayer ? hnswStorageInfo.upperRelTableID : hnswStorageInfo.lowerRelTableID;
    const auto scanState = graph->prepareRelScan(*relTableEntry, relTableID, indexInfo.tableID, {});
    const auto itr = graph->scanFwd(common::nodeID_t{offset, indexInfo.tableID}, *scanState);
    const auto numRels = itr.count();
    if (numRels > static_cast<uint64_t>(maxDegree)) {
        if (numRels >= static_cast<uint64_t>(HNSWIndex::getDegreeThresholdToShrink(maxDegree))) {
            // If the number of existing rels exceeds the threshold, we need to shrink the rels
            // right away.
            shrinkForNode(transaction, offset, isUpperLayer, maxDegree, insertState);
            isUpperLayer ? insertState.upperNodesToShrink.erase(offset) :
                           insertState.lowerNodesToShrink.erase(offset);
        } else {
            isUpperLayer ? insertState.upperNodesToShrink.insert(offset) :
                           insertState.lowerNodesToShrink.insert(offset);
        }
    }
}

void OnDiskHNSWIndex::shrinkForNode(Transaction* transaction, common::offset_t offset,
    bool isUpperLayer, common::length_t maxDegree, HNSWInsertState& insertState) {
    const auto& embeddings = *insertState.searchState.embeddings;
    auto& embeddingScanState = insertState.searchState.embeddingScanState;
    const auto vector = embeddings.getEmbedding(offset, embeddingScanState);
    KU_ASSERT(!vector.isNull());
    const auto& searchState = insertState.searchState;
    const auto& graph = isUpperLayer ? searchState.upperGraph : searchState.lowerGraph;
    const auto relTableID = isUpperLayer ? storageInfo->cast<HNSWStorageInfo>().upperRelTableID :
                                           storageInfo->cast<HNSWStorageInfo>().lowerRelTableID;
    const auto relTableEntry =
        isUpperLayer ? searchState.upperRelTableEntry : searchState.lowerRelTableEntry;
    const auto scanState = graph->prepareRelScan(*relTableEntry, relTableID, indexInfo.tableID, {});
    auto itr = graph->scanFwd(common::nodeID_t{offset, indexInfo.tableID}, *scanState);
    std::vector<common::offset_t> nbrOffsets;
    for (const auto& neighborChunk : itr) {
        neighborChunk.forEachBreakWhenFalse([&](auto neighbors, auto i) -> bool {
            auto nbr = neighbors[i];
            if (nbr.offset == common::INVALID_OFFSET) {
                return false; // End of neighbors.
            }
            nbrOffsets.push_back(nbr.offset);
            return true;
        });
    }

    std::vector<NodeWithDistanceAndEmbedding> nbrs;
    nbrs.reserve(nbrOffsets.size());
    {
        auto nbrVectors = embeddings.getEmbeddings(nbrOffsets, embeddingScanState);
        for (size_t i = 0; i < nbrOffsets.size(); i++) {
            if (nbrVectors[i].isNull()) {
                continue;
            }
            auto dist =
                metricFunc(vector.getPtr(), nbrVectors[i].getPtr(), embeddings.getDimension());
            nbrs.emplace_back(nbrOffsets[i], dist, std::move(nbrVectors[i]));
        }
    }

    std::ranges::sort(nbrs,
        [](const auto& n1, const auto& n2) { return n1.getDist() < n2.getDist(); });
    // First, delete all existing rels for the node.
    insertState.relDeleteState->srcNodeIDVector.setValue(0,
        common::nodeID_t{offset, indexInfo.tableID});
    auto& relTable = isUpperLayer ? *upperRelTable : *lowerRelTable;
    insertState.relDeleteState->detachDeleteDirection = common::RelDataDirection::FWD;
    relTable.detachDelete(transaction, insertState.relDeleteState.get());
    // Perform the actual shrinking and insertion of shrinked rels.
    uint16_t newSize = 0;
    for (auto i = 1u; i < nbrs.size(); i++) {
        bool keepNbr = true;
        for (auto j = i + 1; j < nbrs.size(); j++) {
            KU_ASSERT(checkEmbeddingValidity(nbrs, i, embeddings, embeddingScanState));
            KU_ASSERT(checkEmbeddingValidity(nbrs, j, embeddings, embeddingScanState));
            const auto dist = metricFunc(nbrs[i].embedding.getPtr(), nbrs[j].embedding.getPtr(),
                embeddings.getDimension());
            if (config.alpha * dist < nbrs[i].getDist()) {
                keepNbr = false;
                break;
            }
        }
        if (keepNbr) {
            insertState.relInsertState->srcNodeIDVector.setValue(0,
                common::nodeID_t{offset, indexInfo.tableID});
            insertState.relInsertState->dstNodeIDVector.setValue(0,
                common::nodeID_t{nbrs[i].getNodeOffset(), indexInfo.tableID});
            relTable.insert(transaction, *insertState.relInsertState);
            newSize++;
            if (newSize == maxDegree) {
                break;
            }
        }
    }
}

void OnDiskHNSWIndex::processSecondHopCandidates(const EmbeddingHandle& queryVector,
    HNSWSearchState& searchState, int64_t& numVisitedNbrs, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& results,
    const std::vector<common::offset_t>& candidateOffsets) const {
    for (const auto cand : candidateOffsets) {
        if (!searchOverSecondHopNbrs(queryVector, searchState.ef, searchState, cand, numVisitedNbrs,
                candidates, results)) {
            return;
        }
    }
}

void OnDiskHNSWIndex::processSecondHopCandidates(const EmbeddingHandle& queryVector,
    HNSWSearchState& searchState, int64_t& numVisitedNbrs, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& results, min_node_priority_queue_t& candidatesQueue) const {
    while (!candidatesQueue.empty()) {
        auto [candidate, candidateDist] = candidatesQueue.top();
        candidatesQueue.pop();
        if (!searchOverSecondHopNbrs(queryVector, searchState.ef, searchState, candidate,
                numVisitedNbrs, candidates, results)) {
            return;
        }
    }
}

bool OnDiskHNSWIndex::searchOverSecondHopNbrs(const EmbeddingHandle& queryVector, uint64_t ef,
    HNSWSearchState& searchState, common::offset_t cand, int64_t& numVisitedNbrs,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    if (numVisitedNbrs >= config.ml) {
        return false;
    }
    if (searchState.visited.contains(cand)) {
        return true;
    }
    searchState.visited.add(cand);
    // Second hop lookups from the current node.
    auto secondHopNbrItr = searchState.lowerGraph->scanFwd(
        common::nodeID_t{cand, indexInfo.tableID}, *searchState.secondHopNbrScanState);
    for (const auto& secondHopNbrChunk : secondHopNbrItr) {
        if (numVisitedNbrs >= config.ml) {
            return false;
        }
        secondHopNbrChunk.forEachBreakWhenFalse([&](auto neighbors, auto i) -> bool {
            auto nbr = neighbors[i];
            if (!searchState.visited.contains(nbr.offset) && searchState.isMasked(nbr.offset)) {
                auto nbrVector = searchState.embeddings->getEmbedding(nbr.offset,
                    searchState.embeddingScanState);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, ef,
                    searchState.visited, metricFunc, searchState.embeddings->getDimension(),
                    candidates, results);
                numVisitedNbrs++;
                if (numVisitedNbrs >= config.ml) {
                    return false;
                }
            }
            return true;
        });
    }
    return true;
}

} // namespace vector_extension
} // namespace kuzu
