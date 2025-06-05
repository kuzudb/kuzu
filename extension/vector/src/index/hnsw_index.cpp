#include "index/hnsw_index.h"

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

using namespace kuzu::storage;

namespace kuzu {
namespace vector_extension {

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void HNSWIndexPartitionerSharedState::setTables(NodeTable* nodeTable, RelTable* relTable) {
    lowerPartitionerSharedState->srcNodeTable = nodeTable;
    lowerPartitionerSharedState->dstNodeTable = nodeTable;
    lowerPartitionerSharedState->relTable = relTable;
    upperPartitionerSharedState->srcNodeTable = nodeTable;
    upperPartitionerSharedState->dstNodeTable = nodeTable;
    upperPartitionerSharedState->relTable = relTable;
}

InMemHNSWLayer::InMemHNSWLayer(MemoryManager* mm, InMemHNSWLayerInfo info)
    : entryPoint{common::INVALID_OFFSET}, info{info} {
    graph = std::make_unique<InMemHNSWGraph>(mm, info.numNodes, info.degreeThresholdToShrink);
}

void InMemHNSWLayer::insert(common::offset_t offset, common::offset_t entryPoint_,
    VisitedState& visited) {
    if (entryPoint_ == common::INVALID_OFFSET) {
        const auto entryPointInCurrentLayer = compareAndSwapEntryPoint(offset);
        if (entryPointInCurrentLayer == common::INVALID_OFFSET) {
            // The layer is empty. No edges need to be created.
            return;
        }
        entryPoint_ = entryPointInCurrentLayer;
    }
    const auto closest =
        searchKNN(info.getEmbedding(offset), entryPoint_, info.maxDegree, info.efc, visited);
    for (const auto& n : closest) {
        insertRel(offset, n.nodeOffset);
        insertRel(n.nodeOffset, offset);
    }
}

common::offset_t InMemHNSWLayer::searchNN(void* queryVector, common::offset_t entryNode) const {
    auto currentNodeOffset = entryNode;
    if (entryNode == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    const auto currNodeVector = info.getEmbedding(currentNodeOffset);
    auto minDist = info.metricFunc(queryVector, currNodeVector, info.getDimension());
    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto neighbors = graph->getNeighbors(currentNodeOffset);
        for (const auto nbrOffset : neighbors) {
            if (nbrOffset == graph->getInvalidOffset()) {
                break;
            }
            const auto nbrVector = info.getEmbedding(nbrOffset);
            const auto dist = info.metricFunc(queryVector, nbrVector, info.getDimension());
            if (dist < minDist) {
                minDist = dist;
                currentNodeOffset = nbrOffset;
            }
        }
    }
    return currentNodeOffset;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::insertRel(common::offset_t srcNode, common::offset_t dstNode) {
    const auto currentLen = graph->incrementCSRLength(srcNode);
    KU_ASSERT(srcNode < info.numNodes);
    graph->setDstNode(srcNode * info.degreeThresholdToShrink + currentLen, dstNode);
    if (currentLen == info.degreeThresholdToShrink - 1) {
        shrinkForNode(info, graph.get(), srcNode, currentLen);
    }
}

static void processEntryNodeInKNNSearch(const void* queryVector, const void* entryVector,
    common::offset_t entryNode, VisitedState& visited, const metric_func_t& metricFunc,
    uint64_t dimension, min_node_priority_queue_t& candidates, max_node_priority_queue_t& result) {
    auto dist = metricFunc(queryVector, entryVector, dimension);
    candidates.push({entryNode, dist});
    result.push({entryNode, dist});
    visited.add(entryNode);
}

static void processNbrNodeInKNNSearch(const void* queryVector, const void* nbrVector,
    common::offset_t nbrOffset, uint64_t ef, VisitedState& visited, const metric_func_t& metricFunc,
    uint64_t dimension, min_node_priority_queue_t& candidates, max_node_priority_queue_t& result) {
    visited.add(nbrOffset);
    auto dist = metricFunc(queryVector, nbrVector, dimension);
    if (result.size() < ef || dist < result.top().distance) {
        if (result.size() >= ef) {
            result.pop();
        }
        result.push({nbrOffset, dist});
        candidates.push({nbrOffset, dist});
    }
}

std::vector<NodeWithDistance> InMemHNSWLayer::searchKNN(const void* queryVector,
    common::offset_t entryNode, common::length_t k, uint64_t configuredEf,
    VisitedState& visited) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    visited.reset();
    const auto entryVector = info.getEmbedding(entryNode);
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
        auto neighbors = graph->getNeighbors(candidate);
        for (const auto nbrOffset : neighbors) {
            if (nbrOffset == graph->getInvalidOffset()) {
                break;
            }
            if (!visited.contains(nbrOffset)) {
                const auto nbrVector = info.getEmbedding(nbrOffset);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbrOffset, ef, visited,
                    info.metricFunc, info.getDimension(), candidates, result);
            }
        }
    }
    return HNSWIndex::popTopK(result, k);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::shrinkForNode(const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph,
    common::offset_t nodeOffset, common::length_t numNbrs) {
    std::vector<NodeWithDistance> nbrs;
    const auto vector = info.getEmbedding(nodeOffset);
    const auto neighbors = graph->getNeighbors(nodeOffset);
    nbrs.reserve(numNbrs);
    for (auto nbrOffset : neighbors) {
        if (nbrOffset == graph->getInvalidOffset()) {
            break;
        }
        const auto nbrVector = info.getEmbedding(nbrOffset);
        const auto dist = info.metricFunc(vector, nbrVector, info.getDimension());
        nbrs.emplace_back(nbrOffset, dist);
    }
    std::ranges::sort(nbrs, [](const NodeWithDistance& l, const NodeWithDistance& r) {
        return l.distance < r.distance;
    });
    uint16_t newSize = 0;
    for (auto i = 1u; i < nbrs.size(); i++) {
        bool keepNbr = true;
        for (auto j = i + 1; j < nbrs.size(); j++) {
            const auto nbrIVector = info.getEmbedding(nbrs[i].nodeOffset);
            const auto nbrJVector = info.getEmbedding(nbrs[j].nodeOffset);
            const auto dist = info.metricFunc(nbrIVector, nbrJVector, info.getDimension());
            if (info.alpha * dist < nbrs[i].distance) {
                keepNbr = false;
                break;
            }
        }
        if (keepNbr) {
            const auto startCSROffset = nodeOffset * info.degreeThresholdToShrink;
            graph->setDstNode(startCSROffset + newSize++, nbrs[i].nodeOffset);
        }
        if (newSize == info.maxDegree) {
            break;
        }
    }
    graph->setCSRLength(nodeOffset, newSize);
}

void InMemHNSWLayer::finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    const processor::PartitionerSharedState& partitionerSharedState,
    common::offset_t numNodesInTable, const NodeToGraphOffsetMap& selectedNodesMap) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto endNodeOffset =
        std::min(numNodesInTable, startNodeOffset + common::StorageConfig::NODE_GROUP_SIZE);
    const auto startNodeInGraph = selectedNodesMap.nodeToGraphOffset(startNodeOffset);
    const auto endNodeInGraph = selectedNodesMap.nodeToGraphOffset(endNodeOffset);
    for (auto offsetInGraph = startNodeInGraph; offsetInGraph < endNodeInGraph; offsetInGraph++) {
        KU_ASSERT(offsetInGraph < selectedNodesMap.getNumNodesInGraph() &&
                  selectedNodesMap.graphToNodeOffset(offsetInGraph) >= startNodeInGraph &&
                  selectedNodesMap.graphToNodeOffset(offsetInGraph) < endNodeOffset);
        const auto numNbrs = graph->getCSRLength(offsetInGraph);
        if (numNbrs <= info.maxDegree) {
            continue;
        }
        shrinkForNode(info, graph.get(), offsetInGraph, numNbrs);
    }
    graph->finalize(mm, nodeGroupIdx, partitionerSharedState, startNodeInGraph, endNodeInGraph,
        numNodesInTable, selectedNodesMap);
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

static int64_t getDegreeThresholdToShrink(int64_t degree) {
    return std::ceil(degree * DEFAULT_DEGREE_THRESHOLD_RATIO);
}

static common::ArrayTypeInfo getArrayTypeInfo(NodeTable& table, common::column_id_t columnID) {
    const auto& columnType = table.getColumn(columnID).getDataType();
    KU_ASSERT(columnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto typeInfo = columnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    return common::ArrayTypeInfo{typeInfo->getChildType().copy(), typeInfo->getNumElements()};
}

InMemHNSWIndex::InMemHNSWIndex(const main::ClientContext* context, IndexInfo indexInfo,
    std::unique_ptr<IndexStorageInfo> storageInfo, NodeTable& table, common::column_id_t columnID,
    HNSWIndexConfig config)
    : HNSWIndex{std::move(indexInfo), std::move(storageInfo), std::move(config),
          getArrayTypeInfo(table, columnID)} {
    const auto numNodes = table.getNumTotalRows(context->getTransaction());
    embeddings = std::make_unique<InMemEmbeddings>(context->getTransaction(),
        common::ArrayTypeInfo{typeInfo.getChildType().copy(), typeInfo.getNumElements()},
        table.getTableID(), columnID);
    upperLayerSelectionMask = std::make_unique<common::NullMask>(numNodes);
    for (common::offset_t i = 0; i < numNodes; ++i) {
        if (!embeddings->isNull(i)) {
            const auto rand =
                randomEngine.nextRandomInteger(INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
            if (rand <= INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.pu) {
                upperLayerSelectionMask->setNull(i, true);
            }
        }
    }
    lowerGraphSelectionMap = std::make_unique<NodeToGraphOffsetMap>(numNodes);
    upperGraphSelectionMap =
        std::make_unique<NodeToGraphOffsetMap>(numNodes, upperLayerSelectionMask.get());
    lowerLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()),
            this->metricFunc, getDegreeThresholdToShrink(this->config.ml), this->config.ml,
            this->config.alpha, this->config.efc, *lowerGraphSelectionMap});
    upperLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{upperLayerSelectionMask->countNulls(),
            common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()), this->metricFunc,
            getDegreeThresholdToShrink(this->config.mu), this->config.mu, this->config.alpha,
            this->config.efc, *upperGraphSelectionMap});
}

bool InMemHNSWIndex::insert(common::offset_t offset, VisitedState& upperVisited,
    VisitedState& lowerVisited) {
    if (embeddings->isNull(offset)) {
        return false;
    }
    const auto lowerEntryPoint =
        upperLayer->searchNN(embeddings->getEmbedding(offset), upperLayer->getEntryPoint());
    lowerLayer->insert(offset, lowerEntryPoint, lowerVisited);
    if (upperLayerSelectionMask->isNull(offset)) {
        const auto offsetInUpperGraph = upperGraphSelectionMap->nodeToGraphOffset(offset);
        upperLayer->insert(offsetInUpperGraph, upperLayer->getEntryPoint(), upperVisited);
    }
    return true;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    const HNSWIndexPartitionerSharedState& partitionerSharedState) {
    const auto numNodesInTable = lowerLayer->getNumNodes();
    upperLayer->finalize(mm, nodeGroupIdx, *partitionerSharedState.upperPartitionerSharedState,
        numNodesInTable, *upperGraphSelectionMap);
    lowerLayer->finalize(mm, nodeGroupIdx, *partitionerSharedState.lowerPartitionerSharedState,
        numNodesInTable, *lowerGraphSelectionMap);
}

std::shared_ptr<common::BufferedSerializer> HNSWStorageInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferedSerializer>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.write<common::table_id_t>(upperRelTableID);
    serializer.write<common::table_id_t>(lowerRelTableID);
    serializer.write<common::offset_t>(upperEntryPoint);
    serializer.write<common::offset_t>(lowerEntryPoint);
    return bufferWriter;
}

std::unique_ptr<IndexStorageInfo> HNSWStorageInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::table_id_t upperRelTableID = common::INVALID_TABLE_ID;
    common::table_id_t lowerRelTableID = common::INVALID_TABLE_ID;
    common::offset_t upperEntryPoint = common::INVALID_OFFSET;
    common::offset_t lowerEntryPoint = common::INVALID_OFFSET;
    common::Deserializer deSer{std::move(reader)};
    deSer.deserializeValue<common::table_id_t>(upperRelTableID);
    deSer.deserializeValue<common::table_id_t>(lowerRelTableID);
    deSer.deserializeValue<common::offset_t>(upperEntryPoint);
    deSer.deserializeValue<common::offset_t>(lowerEntryPoint);
    return std::make_unique<HNSWStorageInfo>(upperRelTableID, lowerRelTableID, upperEntryPoint,
        lowerEntryPoint);
}

OnDiskHNSWIndex::OnDiskHNSWIndex(main::ClientContext* context, IndexInfo indexInfo,
    std::unique_ptr<IndexStorageInfo> storageInfo, catalog::NodeTableCatalogEntry* nodeTableEntry,
    catalog::RelGroupCatalogEntry* upperRelTableEntry,
    catalog::RelGroupCatalogEntry* lowerRelTableEntry, HNSWIndexConfig config)
    : HNSWIndex{indexInfo, std::move(storageInfo), std::move(config),
          getArrayTypeInfo(context->getStorageManager()
                               ->getTable(nodeTableEntry->getTableID())
                               ->cast<NodeTable>(),
              indexInfo.columnIDs[0])},
      nodeTable{
          context->getStorageManager()->getTable(nodeTableEntry->getTableID())->cast<NodeTable>()},
      upperRelTableEntry{upperRelTableEntry}, lowerRelTableEntry{lowerRelTableEntry} {
    KU_ASSERT(this->indexInfo.columnIDs.size() == 1);
    KU_ASSERT(nodeTable.getColumn(this->indexInfo.columnIDs[0]).getDataType().getLogicalTypeID() ==
              common::LogicalTypeID::ARRAY);
    embeddings = std::make_unique<OnDiskEmbeddings>(
        common::ArrayTypeInfo{typeInfo.getChildType().copy(), typeInfo.getNumElements()},
        nodeTable);
    graph::GraphEntry lowerGraphEntry{{nodeTableEntry}, {lowerRelTableEntry}};
    lowerGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(lowerGraphEntry));
    graph::GraphEntry upperGraphEntry{{nodeTableEntry}, {upperRelTableEntry}};
    upperGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(upperGraphEntry));
    const auto storageManager = context->getStorageManager();
    lowerRelTable = storageManager->getTable(lowerRelTableEntry->getSingleRelEntryInfo().oid)
                        ->ptrCast<RelTable>();
    upperRelTable = storageManager->getTable(upperRelTableEntry->getSingleRelEntryInfo().oid)
                        ->ptrCast<RelTable>();
}

std::unique_ptr<Index> OnDiskHNSWIndex::load(main::ClientContext* context, StorageManager*,
    IndexInfo indexInfo, std::span<uint8_t> storageInfoBuffer) {
    auto reader =
        std::make_unique<common::BufferReader>(storageInfoBuffer.data(), storageInfoBuffer.size());
    auto storageInfo = HNSWStorageInfo::deserialize(std::move(reader));
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    const auto catalog = context->getCatalog();
    const auto indexEntry =
        catalog->getIndex(&transaction::DUMMY_TRANSACTION, indexInfo.tableID, indexInfo.name);
    auto nodeEntry =
        catalog->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, indexEntry->getTableID())
            ->ptrCast<catalog::NodeTableCatalogEntry>();
    auto upperRelTableEntry =
        catalog
            ->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, hnswStorageInfo.upperRelTableID)
            ->ptrCast<catalog::RelGroupCatalogEntry>();
    auto lowerRelTableEntry =
        catalog
            ->getTableCatalogEntry(&transaction::DUMMY_TRANSACTION, hnswStorageInfo.lowerRelTableID)
            ->ptrCast<catalog::RelGroupCatalogEntry>();
    const auto auxInfo = indexEntry->getAuxInfo().cast<HNSWIndexAuxInfo>();
    return std::make_unique<OnDiskHNSWIndex>(context, std::move(indexInfo), std::move(storageInfo),
        nodeEntry, upperRelTableEntry, lowerRelTableEntry, auxInfo.config.copy());
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::search(transaction::Transaction* transaction,
    const void* queryVector, HNSWSearchState& searchState) const {
    auto entryPoint =
        searchNNInUpperLayer(transaction, queryVector, *searchState.embeddingScanState.scanState);
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    if (entryPoint == common::INVALID_OFFSET) {
        if (hnswStorageInfo.lowerEntryPoint == common::INVALID_OFFSET) {
            // Both upper and lower layers are empty. Thus, the index is empty.
            return {};
        }
        entryPoint = hnswStorageInfo.lowerEntryPoint;
    }
    KU_ASSERT(entryPoint != common::INVALID_OFFSET);
    return searchKNNInLowerLayer(transaction, queryVector, entryPoint, searchState);
}

common::offset_t OnDiskHNSWIndex::searchNNInUpperLayer(transaction::Transaction* transaction,
    const void* queryVector, NodeTableScanState& embeddingScanState) const {
    const auto& hnswStorageInfo = storageInfo->cast<HNSWStorageInfo>();
    auto currentNodeOffset = hnswStorageInfo.upperEntryPoint;
    if (currentNodeOffset == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    const auto currNodeVector =
        embeddings->getEmbedding(transaction, embeddingScanState, currentNodeOffset);
    auto minDist = metricFunc(queryVector, currNodeVector, embeddings->getDimension());
    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    const auto relEntryInfo = upperRelTableEntry->getSingleRelEntryInfo();
    const auto scanState = upperGraph->prepareRelScan(*upperRelTableEntry, relEntryInfo.oid,
        relEntryInfo.nodePair.dstTableID, {} /* relProperties */);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto neighborItr =
            upperGraph->scanFwd(common::nodeID_t{currentNodeOffset, indexInfo.tableID}, *scanState);
        for (const auto neighborChunk : neighborItr) {
            neighborChunk.forEach([&](auto neighbors, auto, auto i) {
                auto neighbor = neighbors[i];
                const auto nbrVector =
                    embeddings->getEmbedding(transaction, embeddingScanState, neighbor.offset);
                const auto dist = metricFunc(queryVector, nbrVector, embeddings->getDimension());
                if (dist < minDist) {
                    minDist = dist;
                    currentNodeOffset = neighbor.offset;
                }
            });
        }
    }
    return currentNodeOffset;
}

void OnDiskHNSWIndex::initLowerLayerSearchState(transaction::Transaction* transaction,
    HNSWSearchState& searchState) const {
    searchState.visited.reset();
    const auto relEntryInfo = lowerRelTableEntry->getSingleRelEntryInfo();
    searchState.nbrScanState = lowerGraph->prepareRelScan(*lowerRelTableEntry, relEntryInfo.oid,
        relEntryInfo.nodePair.dstTableID, {} /* relProperties */);
    searchState.searchType = getFilteredSearchType(transaction, searchState);
    if (searchState.searchType == SearchType::BLIND_TWO_HOP ||
        searchState.searchType == SearchType::DIRECTED_TWO_HOP) {
        searchState.secondHopNbrScanState = lowerGraph->prepareRelScan(*lowerRelTableEntry,
            relEntryInfo.oid, relEntryInfo.nodePair.dstTableID, {} /* relProperties */);
    }
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchKNNInLowerLayer(
    transaction::Transaction* transaction, const void* queryVector, common::offset_t entryNode,
    HNSWSearchState& searchState) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t results;
    initLowerLayerSearchState(transaction, searchState);

    const auto entryVector =
        embeddings->getEmbedding(transaction, *searchState.embeddingScanState.scanState, entryNode);
    auto dist = metricFunc(queryVector, entryVector, embeddings->getDimension());
    candidates.push({entryNode, dist});
    searchState.visited.add(entryNode);
    if (searchState.isMasked(entryNode)) {
        results.push({entryNode, dist});
    }
    initSearchCandidates(transaction, queryVector, searchState, candidates, results);

    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (results.size() >= searchState.ef && candidateDist > results.top().distance) {
            break;
        }
        candidates.pop();
        auto neighborItr = lowerGraph->scanFwd(common::nodeID_t{candidate, indexInfo.tableID},
            *searchState.nbrScanState);
        switch (searchState.searchType) {
        case SearchType::UNFILTERED:
        case SearchType::ONE_HOP_FILTERED: {
            oneHopSearch(transaction, queryVector, neighborItr, searchState, candidates, results);
        } break;
        case SearchType::DIRECTED_TWO_HOP: {
            directedTwoHopFilteredSearch(transaction, queryVector, neighborItr, searchState,
                candidates, results);
        } break;
        case SearchType::BLIND_TWO_HOP: {
            blindTwoHopFilteredSearch(transaction, queryVector, neighborItr, searchState,
                candidates, results);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    return popTopK(results, searchState.k);
}

SearchType OnDiskHNSWIndex::getFilteredSearchType(transaction::Transaction* transaction,
    const HNSWSearchState& searchState) const {
    if (!searchState.hasMask()) {
        return SearchType::UNFILTERED;
    }
    const auto selectivity =
        1.0 * searchState.semiMask->getNumMaskedNodes() / lowerGraph->getNumNodes(transaction);
    if (selectivity < searchState.config.blindSearchUpSelThreshold) {
        return SearchType::BLIND_TWO_HOP;
    }
    if (selectivity < searchState.config.directedSearchUpSelThreshold) {
        return SearchType::DIRECTED_TWO_HOP;
    }
    return SearchType::ONE_HOP_FILTERED;
}

void OnDiskHNSWIndex::initSearchCandidates(transaction::Transaction* transaction,
    const void* queryVector, HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
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
            const auto candidateVector = embeddings->getEmbedding(transaction,
                *searchState.embeddingScanState.scanState, candidate);
            auto candidateDist =
                metricFunc(queryVector, candidateVector, embeddings->getDimension());
            candidates.push({candidate, candidateDist});
            results.push({candidate, candidateDist});
        }
    } break;
    default: {
        // DO NOTHING
    }
    }
}

void OnDiskHNSWIndex::oneHopSearch(transaction::Transaction* transaction, const void* queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    for (const auto neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbors, auto, auto i) {
            const auto nbr = neighbors[i];
            if (!searchState.visited.contains(nbr.offset) && searchState.isMasked(nbr.offset)) {
                const auto nbrVector = embeddings->getEmbedding(transaction,
                    *searchState.embeddingScanState.scanState, nbr.offset);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, searchState.ef,
                    searchState.visited, metricFunc, embeddings->getDimension(), candidates,
                    results);
            }
        });
    }
}

void OnDiskHNSWIndex::directedTwoHopFilteredSearch(transaction::Transaction* transaction,
    const void* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    int64_t numVisitedNbrs = 0;
    auto candidatesForSecHop = collectFirstHopNbrsDirected(transaction, queryVector, nbrItr,
        searchState, candidates, results, numVisitedNbrs);
    processSecondHopCandidates(transaction, queryVector, searchState, numVisitedNbrs, candidates,
        results, candidatesForSecHop);
}

min_node_priority_queue_t OnDiskHNSWIndex::collectFirstHopNbrsDirected(
    transaction::Transaction* transaction, const void* queryVector,
    graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
    int64_t& numVisitedNbrs) const {
    min_node_priority_queue_t candidatesForSecHop;
    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbors, auto, auto i) {
            const auto neighbor = neighbors[i];
            auto nbrOffset = neighbor.offset;
            if (!searchState.visited.contains(nbrOffset)) {
                const auto nbrVector = embeddings->getEmbedding(transaction,
                    *searchState.embeddingScanState.scanState, nbrOffset);
                auto dist = metricFunc(queryVector, nbrVector, embeddings->getDimension());
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
        });
    }
    return candidatesForSecHop;
}

void OnDiskHNSWIndex::blindTwoHopFilteredSearch(transaction::Transaction* transaction,
    const void* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const {
    int64_t numVisitedNbrs = 0;
    const auto secondHopCandidates = collectFirstHopNbrsBlind(transaction, queryVector, nbrItr,
        searchState, candidates, results, numVisitedNbrs);
    processSecondHopCandidates(transaction, queryVector, searchState, numVisitedNbrs, candidates,
        results, secondHopCandidates);
}

common::offset_vec_t OnDiskHNSWIndex::collectFirstHopNbrsBlind(
    transaction::Transaction* transaction, const void* queryVector,
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
                    auto nbrVector = embeddings->getEmbedding(transaction,
                        *searchState.embeddingScanState.scanState, nbr.offset);
                    processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, searchState.ef,
                        searchState.visited, metricFunc, embeddings->getDimension(), candidates,
                        results);
                }
            }
        });
    }
    return secondHopCandidates;
}

void OnDiskHNSWIndex::processSecondHopCandidates(transaction::Transaction* transaction,
    const void* queryVector, HNSWSearchState& searchState, int64_t& numVisitedNbrs,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
    const std::vector<common::offset_t>& candidateOffsets) const {
    for (const auto cand : candidateOffsets) {
        if (!searchOverSecondHopNbrs(transaction, queryVector, searchState.ef, searchState, cand,
                numVisitedNbrs, candidates, results)) {
            return;
        }
    }
}

void OnDiskHNSWIndex::processSecondHopCandidates(transaction::Transaction* transaction,
    const void* queryVector, HNSWSearchState& searchState, int64_t& numVisitedNbrs,
    min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
    min_node_priority_queue_t& candidatesQueue) const {
    while (!candidatesQueue.empty()) {
        auto [candidate, candidateDist] = candidatesQueue.top();
        candidatesQueue.pop();
        if (!searchOverSecondHopNbrs(transaction, queryVector, searchState.ef, searchState,
                candidate, numVisitedNbrs, candidates, results)) {
            return;
        }
    }
}

bool OnDiskHNSWIndex::searchOverSecondHopNbrs(transaction::Transaction* transaction,
    const void* queryVector, uint64_t ef, HNSWSearchState& searchState, common::offset_t cand,
    int64_t& numVisitedNbrs, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& results) const {
    if (numVisitedNbrs >= config.ml) {
        return false;
    }
    if (searchState.visited.contains(cand)) {
        return true;
    }
    searchState.visited.add(cand);
    // Second hop lookups from the current node.
    auto secondHopNbrItr = lowerGraph->scanFwd(common::nodeID_t{cand, indexInfo.tableID},
        *searchState.secondHopNbrScanState);
    for (const auto& secondHopNbrChunk : secondHopNbrItr) {
        if (numVisitedNbrs >= config.ml) {
            return false;
        }
        secondHopNbrChunk.forEachBreakWhenFalse([&](auto neighbors, auto i) -> bool {
            auto nbr = neighbors[i];
            if (!searchState.visited.contains(nbr.offset) && searchState.isMasked(nbr.offset)) {
                auto nbrVector = embeddings->getEmbedding(transaction,
                    *searchState.embeddingScanState.scanState, nbr.offset);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbr.offset, ef,
                    searchState.visited, metricFunc, embeddings->getDimension(), candidates,
                    results);
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
