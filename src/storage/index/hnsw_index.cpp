#include "storage/index/hnsw_index.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "main/client_context.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {

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
        const auto entryPointInCurrentLayer = getEntryPoint();
        if (entryPointInCurrentLayer == common::INVALID_OFFSET) {
            setEntryPoint(offset);
            // The layer is empty. No edges need to be created.
            return;
        }
        entryPoint_ = entryPointInCurrentLayer;
    }
    const auto closest = searchKNN(info.embeddings->getEmbedding(offset), entryPoint_,
        info.maxDegree, info.efc, visited);
    for (const auto& n : closest) {
        insertRel(offset, n.nodeOffset);
        insertRel(n.nodeOffset, offset);
    }
}

common::offset_t InMemHNSWLayer::searchNN(common::offset_t node, common::offset_t entryNode) const {
    auto currentNodeOffset = entryNode;
    if (entryNode == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    const auto queryVector = info.embeddings->getEmbedding(node);
    const auto currNodeVector = info.embeddings->getEmbedding(currentNodeOffset);
    auto minDist = HNSWIndexUtils::computeDistance(info.distFunc, queryVector, currNodeVector,
        info.embeddings->getDimension());
    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto neighbors = graph->getNeighbors(currentNodeOffset);
        for (const auto& nbr : neighbors) {
            const auto nbrOffset = nbr.load(std::memory_order_relaxed);
            if (nbrOffset == common::INVALID_OFFSET) {
                break;
            }
            const auto nbrVector = info.embeddings->getEmbedding(nbrOffset);
            const auto dist = HNSWIndexUtils::computeDistance(info.distFunc, queryVector, nbrVector,
                info.embeddings->getDimension());
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
    if (currentLen >= info.degreeThresholdToShrink) {
        shrinkForNode(info, graph.get(), srcNode, currentLen);
    } else {
        KU_ASSERT(srcNode < info.numNodes);
        graph->setDstNode(srcNode * info.degreeThresholdToShrink + currentLen, dstNode);
    }
}

static void processEntryNodeInKNNSearch(const float* queryVector, const float* entryVector,
    common::offset_t entryNode, VisitedState& visited, DistFuncType distFunc,
    const EmbeddingColumn* embeddings, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& result) {
    auto dist = HNSWIndexUtils::computeDistance(distFunc, queryVector, entryVector,
        embeddings->getDimension());
    candidates.push({entryNode, dist});
    result.push({entryNode, dist});
    visited.add(entryNode);
}

static void processNbrNodeInKNNSearch(const float* queryVector, const float* nbrVector,
    common::offset_t nbrOffset, uint64_t ef, VisitedState& visited, DistFuncType distFunc,
    const EmbeddingColumn* embeddings, min_node_priority_queue_t& candidates,
    max_node_priority_queue_t& result) {
    visited.add(nbrOffset);
    auto dist = HNSWIndexUtils::computeDistance(distFunc, queryVector, nbrVector,
        embeddings->getDimension());
    if (result.size() < ef || dist < result.top().distance) {
        if (result.size() == ef) {
            result.pop();
        }
        result.push({nbrOffset, dist});
        candidates.push({nbrOffset, dist});
    }
}

std::vector<NodeWithDistance> InMemHNSWLayer::searchKNN(const float* queryVector,
    common::offset_t entryNode, common::length_t k, uint64_t configuredEf,
    VisitedState& visited) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    visited.reset();
    const auto entryVector = info.embeddings->getEmbedding(entryNode);
    processEntryNodeInKNNSearch(queryVector, entryVector, entryNode, visited, info.distFunc,
        info.embeddings, candidates, result);
    const auto ef = std::max(k, configuredEf);
    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (result.size() >= ef && candidateDist > result.top().distance) {
            break;
        }
        candidates.pop();
        auto neighbors = graph->getNeighbors(candidate);
        for (const auto& nbr : neighbors) {
            const auto nbrOffset = nbr.load(std::memory_order_relaxed);
            if (nbrOffset == common::INVALID_OFFSET) {
                break;
            }
            if (!visited.contains(nbrOffset)) {
                const auto nbrVector = info.embeddings->getEmbedding(nbrOffset);
                processNbrNodeInKNNSearch(queryVector, nbrVector, nbrOffset, ef, visited,
                    info.distFunc, info.embeddings, candidates, result);
            }
        }
    }
    return HNSWIndex::popTopK(result, k);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::shrinkForNode(const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph,
    common::offset_t nodeOffset, common::length_t numNbrs) {
    std::vector<NodeWithDistance> nbrs;
    const auto vector = info.embeddings->getEmbedding(nodeOffset);
    const auto neighbors = graph->getNeighbors(nodeOffset);
    nbrs.reserve(numNbrs);
    for (auto i = 0u; i < numNbrs; i++) {
        auto& nbr = neighbors[i];
        auto nbrOffset = nbr.load(std::memory_order_relaxed);
        if (nbrOffset == common::INVALID_OFFSET) {
            break;
        }
        const auto nbrVector = info.embeddings->getEmbedding(nbrOffset);
        const auto dist = HNSWIndexUtils::computeDistance(info.distFunc, vector, nbrVector,
            info.embeddings->getDimension());
        nbrs.emplace_back(nbrOffset, dist);
    }
    std::ranges::sort(nbrs, [](const NodeWithDistance& l, const NodeWithDistance& r) {
        return l.distance < r.distance;
    });
    uint16_t newSize = 0;
    for (auto i = 1u; i < nbrs.size(); i++) {
        bool keepNbr = true;
        for (auto j = i + 1; j < nbrs.size(); j++) {
            const auto nbrIVector = info.embeddings->getEmbedding(nbrs[i].nodeOffset);
            const auto nbrJVector = info.embeddings->getEmbedding(nbrs[j].nodeOffset);
            const auto dist = HNSWIndexUtils::computeDistance(info.distFunc, nbrIVector, nbrJVector,
                info.embeddings->getDimension());
            if (info.alpha * dist < nbrs[i].distance) {
                keepNbr = false;
                break;
            }
        }
        if (keepNbr) {
            graph->setDstNode(newSize++, nbrs[i].nodeOffset);
        }
        if (newSize == info.maxDegree) {
            break;
        }
    }
    graph->setCSRLength(nodeOffset, newSize);
}

void InMemHNSWLayer::finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    const processor::PartitionerSharedState& partitionerSharedState) const {
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto numNodesInGroup =
        std::min(common::StorageConfig::NODE_GROUP_SIZE, info.numNodes - startNodeOffset);
    for (auto i = 0u; i < numNodesInGroup; i++) {
        const auto nodeOffset = startNodeOffset + i;
        const auto numNbrs = graph->getCSRLength(nodeOffset);
        if (numNbrs <= info.maxDegree) {
            continue;
        }
        shrinkForNode(info, graph.get(), nodeOffset, numNbrs);
    }
    graph->finalize(mm, nodeGroupIdx, partitionerSharedState);
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
    return std::ceil(degree * 1.25);
}

InMemHNSWIndex::InMemHNSWIndex(const main::ClientContext* context, NodeTable& table,
    common::column_id_t columnID, HNSWIndexConfig config)
    : HNSWIndex{std::move(config)} {
    const auto& columnType = table.getColumn(columnID).getDataType();
    KU_ASSERT(columnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto extraInfo = columnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    EmbeddingTypeInfo typeInfo{extraInfo->getChildType().copy(), extraInfo->getNumElements()};
    const auto numNodes = table.getNumTotalRows(context->getTransaction());
    embeddings = std::make_unique<InMemEmbeddings>(context->getTransaction(), std::move(typeInfo),
        table.getTableID(), columnID);
    lowerLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()),
            this->config.distFunc, getDegreeThresholdToShrink(this->config.ml), this->config.ml,
            this->config.alpha, this->config.efc});
    upperLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()),
            this->config.distFunc, getDegreeThresholdToShrink(this->config.mu), this->config.mu,
            this->config.alpha, this->config.efc});
}

void InMemHNSWIndex::insert(common::offset_t offset, VisitedState& upperVisited,
    VisitedState& lowerVisited) {
    const auto lowerEntryPoint = upperLayer->searchNN(offset, upperLayer->getEntryPoint());
    lowerLayer->insert(offset, lowerEntryPoint, lowerVisited);
    const auto rand = randomEngine.nextRandomInteger(INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
    if (rand <= INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.pu) {
        upperLayer->insert(offset, upperLayer->getEntryPoint(), upperVisited);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
    const HNSWIndexPartitionerSharedState& partitionerSharedState) {
    upperLayer->finalize(mm, nodeGroupIdx, *partitionerSharedState.upperPartitionerSharedState);
    lowerLayer->finalize(mm, nodeGroupIdx, *partitionerSharedState.lowerPartitionerSharedState);
}

OnDiskHNSWIndex::OnDiskHNSWIndex(main::ClientContext* context,
    catalog::NodeTableCatalogEntry* nodeTableEntry, common::column_id_t columnID,
    catalog::RelTableCatalogEntry* upperRelTableEntry,
    catalog::RelTableCatalogEntry* lowerRelTableEntry, HNSWIndexConfig config)
    : HNSWIndex{std::move(config)}, nodeTableID{nodeTableEntry->getTableID()},
      upperRelTableEntry{upperRelTableEntry}, lowerRelTableEntry{lowerRelTableEntry} {
    auto& nodeTable =
        context->getStorageManager()->getTable(nodeTableEntry->getTableID())->cast<NodeTable>();
    const auto& indexColumnType = nodeTable.getColumn(columnID).getDataType();
    KU_ASSERT(indexColumnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto extraTypeInfo =
        indexColumnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    EmbeddingTypeInfo indexColumnTypeInfo{extraTypeInfo->getChildType().copy(),
        extraTypeInfo->getNumElements()};
    embeddings =
        std::make_unique<OnDiskEmbeddings>(std::move(indexColumnTypeInfo), nodeTable, columnID);
    graph::GraphEntry lowerGraphEntry{{nodeTableEntry}, {lowerRelTableEntry}};
    lowerGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(lowerGraphEntry));
    graph::GraphEntry upperGraphEntry{{nodeTableEntry}, {upperRelTableEntry}};
    upperGraph = std::make_unique<graph::OnDiskGraph>(context, std::move(upperGraphEntry));
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::search(transaction::Transaction* transaction,
    const std::vector<float>& queryVector, HNSWSearchState& searchState) const {
    auto entryPoint = searchNNInUpperLayer(transaction, &queryVector[0],
        *searchState.embeddingScanState.scanState);
    if (entryPoint == common::INVALID_OFFSET) {
        if (defaultLowerEntryPoint == common::INVALID_OFFSET) {
            // Both upper and lower layers are empty. Thus, the index is empty.
            return {};
        }
        entryPoint = defaultLowerEntryPoint;
    }
    KU_ASSERT(entryPoint != common::INVALID_OFFSET);
    return searchKNNInLowerLayer(transaction, &queryVector[0], entryPoint, searchState);
}

common::offset_t OnDiskHNSWIndex::searchNNInUpperLayer(transaction::Transaction* transaction,
    const float* queryVector, NodeTableScanState& embeddingScanState) const {
    auto currentNodeOffset = defaultUpperEntryPoint.load();
    if (currentNodeOffset == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    const auto currNodeVector =
        embeddings->getEmbedding(transaction, embeddingScanState, currentNodeOffset);
    auto minDist = HNSWIndexUtils::computeDistance(config.distFunc, queryVector, currNodeVector,
        embeddings->getDimension());
    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto scanState = upperGraph->prepareRelLookup(upperRelTableEntry);
        auto neighborItr =
            upperGraph->scanFwd(common::nodeID_t{currentNodeOffset, nodeTableID}, *scanState);
        for (const auto neighborChunk : neighborItr) {
            neighborChunk.forEach([&](auto neighbor, auto) {
                const auto nbrVector =
                    embeddings->getEmbedding(transaction, embeddingScanState, neighbor.offset);
                const auto dist = HNSWIndexUtils::computeDistance(config.distFunc, queryVector,
                    nbrVector, embeddings->getDimension());
                if (dist < minDist) {
                    minDist = dist;
                    currentNodeOffset = neighbor.offset;
                }
            });
        }
    }
    return currentNodeOffset;
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchKNNInLowerLayer(
    transaction::Transaction* transaction, const float* queryVector, common::offset_t entryNode,
    HNSWSearchState& searchState) const {
    return searchState.semiMask ?
               searchFilteredKNNInLowerLayer(transaction, queryVector, entryNode, searchState) :
               searchUnfilteredKNNInLowerLayer(transaction, queryVector, entryNode, searchState);
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchUnfilteredKNNInLowerLayer(
    transaction::Transaction* transaction, const float* queryVector, common::offset_t entryNode,
    HNSWSearchState& searchState) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    searchState.visited.reset();
    const auto entryVector =
        embeddings->getEmbedding(transaction, *searchState.embeddingScanState.scanState, entryNode);
    processEntryNodeInKNNSearch(queryVector, entryVector, entryNode, searchState.visited,
        config.distFunc, embeddings.get(), candidates, result);
    const auto ef = std::max(searchState.k, static_cast<uint64_t>(searchState.config.efs));
    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (result.size() >= ef && candidateDist > result.top().distance) {
            break;
        }
        candidates.pop();
        auto scanState = lowerGraph->prepareRelLookup(lowerRelTableEntry);
        auto neighborItr =
            lowerGraph->scanFwd(common::nodeID_t{candidate, nodeTableID}, *scanState);
        for (const auto neighborChunk : neighborItr) {
            neighborChunk.forEach([&](auto neighbor, auto) {
                if (!searchState.visited.contains(neighbor.offset)) {
                    const auto nbrVector = embeddings->getEmbedding(transaction,
                        *searchState.embeddingScanState.scanState, neighbor.offset);
                    processNbrNodeInKNNSearch(queryVector, nbrVector, neighbor.offset, ef,
                        searchState.visited, config.distFunc, embeddings.get(), candidates, result);
                }
            });
        }
    }
    return popTopK(result, searchState.k);
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::searchFilteredKNNInLowerLayer(
    transaction::Transaction* transaction, const float* queryVector, common::offset_t entryNode,
    HNSWSearchState& searchState) const {
    KU_ASSERT(searchState.semiMask);
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    searchState.visited.reset();

    // Process entry node.
    const auto entryVector =
        embeddings->getEmbedding(transaction, *searchState.embeddingScanState.scanState, entryNode);
    auto dist = HNSWIndexUtils::computeDistance(config.distFunc, queryVector, entryVector,
        embeddings->getDimension());
    candidates.push({entryNode, dist});
    searchState.visited.add(entryNode);
    if (searchState.semiMask->isMasked(entryNode)) {
        result.push({entryNode, dist});
    }

    const auto selectivity =
        1.0 * searchState.semiMask->getNumMaskedNodes() / lowerGraph->getNumNodes(transaction);
    const auto ef = std::max(searchState.k, static_cast<uint64_t>(searchState.config.efs));

    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (result.size() >= ef && candidateDist > result.top().distance) {
            break;
        }
        candidates.pop();
        auto scanState = lowerGraph->prepareRelLookup(lowerRelTableEntry);
        auto neighborItr =
            lowerGraph->scanFwd(common::nodeID_t{candidate, nodeTableID}, *scanState);
        common::offset_vec_t neighbors;
        if (selectivity < BLIND_SEARCH_UP_SEL_THRESHOLD) {
            // Go for blind search.
            neighbors = blindTwoHopFilteredSearch(searchState.semiMask, ef, neighborItr,
                searchState.visited);
        } else if (selectivity < DIRECTED_SEARCH_UP_SEL_THRESHOLD) {
            // Go for directed search.
            neighbors = directedTwoHopFilteredSearch(transaction, searchState.semiMask, queryVector,
                ef, neighborItr, searchState.visited, *searchState.embeddingScanState.scanState);
        } else {
            // Go for one-hop search.
            neighbors =
                oneHopFilteredSearch(searchState.semiMask, ef, neighborItr, searchState.visited);
        }
        for (auto nbr : neighbors) {
            const auto nbrVector = embeddings->getEmbedding(transaction,
                *searchState.embeddingScanState.scanState, nbr);
            dist = HNSWIndexUtils::computeDistance(config.distFunc, queryVector, nbrVector,
                embeddings->getDimension());
            if (result.size() < ef || dist < result.top().distance) {
                if (result.size() == ef) {
                    result.pop();
                }
                result.push({nbr, dist});
                candidates.push({nbr, dist});
            }
        }
    }

    return popTopK(result, searchState.k);
}

common::offset_vec_t OnDiskHNSWIndex::oneHopFilteredSearch(common::SemiMask* semiMask,
    uint64_t ef, graph::Graph::EdgeIterator& nbrItr, VisitedState& visited) const {
    common::offset_vec_t result;
    result.reserve(ef);
    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbor, auto) {
            if (!visited.contains(neighbor.offset)) {
                visited.add(neighbor.offset);
                if (semiMask->isMasked(neighbor.offset)) {
                    result.push_back(neighbor.offset);
                }
            }
        });
    }
    return result;
}

common::offset_vec_t OnDiskHNSWIndex::directedTwoHopFilteredSearch(
    transaction::Transaction* transaction, common::SemiMask* semiMask, const float* queryVector,
    uint64_t ef, graph::Graph::EdgeIterator& nbrItr, VisitedState& visited,
    NodeTableScanState& embeddingScanState) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t firstHopResult;
    common::offset_vec_t result;

    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbor, auto) {
            auto nbrOffset = neighbor.offset;
            if (!visited.contains(nbrOffset)) {
                const auto nbrVector =
                    embeddings->getEmbedding(transaction, embeddingScanState, neighbor.offset);
                auto dist = HNSWIndexUtils::computeDistance(config.distFunc, queryVector, nbrVector,
                    embeddings->getDimension());
                if (result.size() < ef || dist < firstHopResult.top().distance) {
                    if (result.size() == ef) {
                        firstHopResult.pop();
                    }
                    firstHopResult.push({nbrOffset, dist});
                    candidates.push({nbrOffset, dist});
                }
                if (semiMask->isMasked(neighbor.offset)) {
                    visited.add(nbrOffset);
                    result.push_back(neighbor.offset);
                }
            }
        });
    }

    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        candidates.pop();
        if (result.size() >= ef) {
            break;
        }
        if (visited.contains(candidate)) {
            continue;
        }
        visited.add(candidate);
        auto scanState = lowerGraph->prepareRelLookup(lowerRelTableEntry);
        auto secondHopNbrItr =
            lowerGraph->scanFwd(common::nodeID_t{candidate, nodeTableID}, *scanState);
        for (const auto& secondHopNbrChunk : secondHopNbrItr) {
            secondHopNbrChunk.forEach([&](auto neighbor, auto) {
                if (!visited.contains(neighbor.offset)) {
                    visited.add(neighbor.offset);
                    if (semiMask->isMasked(neighbor.offset)) {
                        result.push_back(neighbor.offset);
                    }
                }
            });
        }
    }
    return result;
}

common::offset_vec_t OnDiskHNSWIndex::blindTwoHopFilteredSearch(common::SemiMask* semiMask,
    uint64_t ef, graph::Graph::EdgeIterator& nbrItr, VisitedState& visited) const {
    common::offset_vec_t result;
    result.reserve(ef);
    for (const auto& neighborChunk : nbrItr) {
        neighborChunk.forEach([&](auto neighbor, auto) {
            // TODO(Guodong): Ideally we should break the outer for loop here.
            if (result.size() >= ef) {
                return;
            }
            if (!visited.contains(neighbor.offset)) {
                visited.add(neighbor.offset);
                if (semiMask->isMasked(neighbor.offset)) {
                    result.push_back(neighbor.offset);
                }
                if (result.size() >= ef) {
                    return;
                }
                // Second hop lookups from the current node.
                const auto secondHopScanState = lowerGraph->prepareRelLookup(lowerRelTableEntry);
                auto secondHopNbrItr = lowerGraph->scanFwd(
                    common::nodeID_t{neighbor.offset, nodeTableID}, *secondHopScanState);
                for (const auto& secondHopNbrChunk : secondHopNbrItr) {
                    secondHopNbrChunk.forEach([&](auto nbr, auto) {
                        if (result.size() >= ef) {
                            return;
                        }
                        if (!visited.contains(nbr.offset)) {
                            visited.add(nbr.offset);
                            if (semiMask->isMasked(nbr.offset)) {
                                result.push_back(nbr.offset);
                            }
                        }
                    });
                }
            }
        });
    }
    return result;
}

} // namespace storage
} // namespace kuzu
