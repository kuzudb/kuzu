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

void InMemHNSWLayer::insert(transaction::Transaction* transaction, common::offset_t offset,
    common::offset_t entryPoint_, VisitedState& visited) {
    if (entryPoint_ == common::INVALID_OFFSET) {
        const auto entryPointInCurrentLayer = getEntryPoint();
        if (entryPointInCurrentLayer == common::INVALID_OFFSET) {
            setEntryPoint(offset);
            // The layer is empty. No edges need to be created.
            return;
        }
        entryPoint_ = entryPointInCurrentLayer;
    }
    const auto closest = searchKNN(transaction, info.embeddings->getEmbedding(offset), entryPoint_,
        info.maxDegree, info.efc, visited);
    for (const auto& n : closest) {
        insertRel(transaction, offset, n.nodeOffset);
        insertRel(transaction, n.nodeOffset, offset);
    }
}

common::offset_t InMemHNSWLayer::searchNN(transaction::Transaction* transaction,
    common::offset_t node, common::offset_t entryNode) const {
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
        auto neighbors = graph->getNeighbors(transaction, currentNodeOffset);
        for (const auto nbr : neighbors) {
            const auto nbrVector = info.embeddings->getEmbedding(nbr);
            const auto dist = HNSWIndexUtils::computeDistance(info.distFunc, queryVector, nbrVector,
                info.embeddings->getDimension());
            if (dist < minDist) {
                minDist = dist;
                currentNodeOffset = nbr;
            }
        }
    }
    return currentNodeOffset;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::insertRel(transaction::Transaction* transaction, common::offset_t srcNode,
    common::offset_t dstNode) {
    const auto currentLen = graph->incrementCSRLength(srcNode);
    if (currentLen >= info.degreeThresholdToShrink) {
        shrinkForNode(transaction, info, graph.get(), srcNode, currentLen);
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

std::vector<NodeWithDistance> InMemHNSWLayer::searchKNN(transaction::Transaction* transaction,
    const float* queryVector, common::offset_t entryNode, common::length_t k, uint64_t configuredEf,
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
        auto neighbors = graph->getNeighbors(transaction, candidate);
        for (const auto neighbor : neighbors) {
            if (!visited.contains(neighbor)) {
                const auto nbrVector = info.embeddings->getEmbedding(neighbor);
                processNbrNodeInKNNSearch(queryVector, nbrVector, neighbor, ef, visited,
                    info.distFunc, info.embeddings, candidates, result);
            }
        }
    }
    return HNSWIndex::popTopK(result, k);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::shrinkForNode(transaction::Transaction* transaction,
    const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph, common::offset_t nodeOffset,
    common::length_t numNbrs) {
    std::vector<NodeWithDistance> nbrs;
    const auto vector = info.embeddings->getEmbedding(nodeOffset);
    const auto neighborOffsets = graph->getNeighbors(transaction, nodeOffset, numNbrs);
    nbrs.reserve(neighborOffsets.size());
    for (const auto nbrOffset : neighborOffsets) {
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

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWLayer::shrink(transaction::Transaction* transaction) {
    for (auto i = 0u; i < info.numNodes; i++) {
        const auto numNbrs = graph->getCSRLength(i);
        if (numNbrs <= info.maxDegree) {
            return;
        }
        shrinkForNode(transaction, info, graph.get(), i, numNbrs);
    }
}

void InMemHNSWLayer::finalize(MemoryManager& mm,
    const processor::PartitionerSharedState& partitionerSharedState) const {
    graph->finalize(mm, partitionerSharedState);
}

std::vector<NodeWithDistance> HNSWIndex::popTopK(max_node_priority_queue_t& result,
    common::length_t k) {
    // Gather top k elements from result priority queue.
    std::vector<NodeWithDistance> topK;
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

InMemHNSWIndex::InMemHNSWIndex(main::ClientContext* context, NodeTable& table,
    common::column_id_t columnID, HNSWIndexConfig config)
    : HNSWIndex{std::move(config)} {
    auto& columnType = table.getColumn(columnID).getDataType();
    KU_ASSERT(columnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto extraInfo = columnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    EmbeddingTypeInfo typeInfo{extraInfo->getChildType().copy(), extraInfo->getNumElements()};
    const auto numNodes = table.getNumTotalRows(context->getTransaction());
    embeddings = std::make_unique<InMemEmbeddings>(*context->getMemoryManager(),
        std::move(typeInfo), numNodes, columnType);
    lowerLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()),
            this->config.distFunc, getDegreeThresholdToShrink(this->config.ml), this->config.ml,
            this->config.alpha, this->config.efc});
    upperLayer = std::make_unique<InMemHNSWLayer>(context->getMemoryManager(),
        InMemHNSWLayerInfo{numNodes, common::ku_dynamic_cast<InMemEmbeddings*>(embeddings.get()),
            this->config.distFunc, getDegreeThresholdToShrink(this->config.mu), this->config.mu,
            this->config.alpha, this->config.efc});
    embeddings->initialize(context, table, columnID);
}

void InMemHNSWIndex::insert(common::offset_t offset, transaction::Transaction* transaction,
    VisitedState& upperVisited, VisitedState& lowerVisited) {
    auto lowerEntryPoint = upperLayer->searchNN(transaction, offset, upperLayer->getEntryPoint());
    lowerLayer->insert(transaction, offset, lowerEntryPoint, lowerVisited);
    const auto rand = randomEngine.nextRandomInteger(INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
    if (rand <= INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.pu) {
        upperLayer->insert(transaction, offset, upperLayer->getEntryPoint(), upperVisited);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::shrink(transaction::Transaction* transaction) {
    upperLayer->shrink(transaction);
    lowerLayer->shrink(transaction);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::finalize(MemoryManager& mm,
    const HNSWIndexPartitionerSharedState& partitionerSharedState) {
    upperLayer->finalize(mm, *partitionerSharedState.upperPartitionerSharedState);
    lowerLayer->finalize(mm, *partitionerSharedState.lowerPartitionerSharedState);
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
    const std::vector<float>& queryVector, common::length_t k, const QueryHNSWConfig& config,
    VisitedState& visited, NodeTableScanState& embeddingScanState) const {
    auto entryPoint = searchNNInUpperLayer(transaction, &queryVector[0], embeddingScanState);
    if (entryPoint == common::INVALID_OFFSET) {
        if (defaultLowerEntryPoint == common::INVALID_OFFSET) {
            // Both upper and lower layers are empty. Thus, the index is empty.
            return {};
        }
        entryPoint = defaultLowerEntryPoint;
    }
    KU_ASSERT(entryPoint != common::INVALID_OFFSET);
    return searchKNNInLowerLayer(transaction, &queryVector[0], entryPoint, k, config.efs, visited,
        embeddingScanState);
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
    common::length_t k, uint64_t configuredEf, VisitedState& visited,
    NodeTableScanState& embeddingScanState) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    visited.reset();
    const auto entryVector = embeddings->getEmbedding(transaction, embeddingScanState, entryNode);
    processEntryNodeInKNNSearch(queryVector, entryVector, entryNode, visited, config.distFunc,
        embeddings.get(), candidates, result);
    const auto ef = std::max(k, configuredEf);
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
                if (!visited.contains(neighbor.offset)) {
                    const auto nbrVector =
                        embeddings->getEmbedding(transaction, embeddingScanState, neighbor.offset);
                    processNbrNodeInKNNSearch(queryVector, nbrVector, neighbor.offset, ef, visited,
                        config.distFunc, embeddings.get(), candidates, result);
                }
            });
        }
    }
    return popTopK(result, k);
}

} // namespace storage
} // namespace kuzu
