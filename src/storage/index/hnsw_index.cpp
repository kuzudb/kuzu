#include "storage/index/hnsw_index.h"

#include "main/client_context.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/store/csr_chunked_node_group.h"
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

std::vector<NodeWithDistance> HNSWIndex::searchLowerLayer(const float* queryVector,
    common::offset_t entryNode, common::length_t k, uint64_t configuredEf,
    transaction::Transaction* transaction, VisitedState& visited) const {
    min_node_priority_queue_t candidates;
    max_node_priority_queue_t result;
    visited.reset();
    auto dist = HNSWIndexUtils::computeDistance(config.distFunc, *embeddings, queryVector,
        entryNode, transaction);
    candidates.push({entryNode, dist});
    result.push({entryNode, dist});
    visited.add(entryNode);
    const auto ef = std::max(k, configuredEf);
    while (!candidates.empty()) {
        auto [candidate, candidateDist] = candidates.top();
        // Break here if adding closestNode to result will exceed efs or not improve the results.
        if (result.size() >= ef && candidateDist > result.top().distance) {
            break;
        }
        candidates.pop();
        auto neighbors = lowerGraph->getNeighbors(transaction, candidate);
        for (const auto neighbor : neighbors) {
            if (visited.contains(neighbor)) {
                continue;
            }
            visited.add(neighbor);
            dist = HNSWIndexUtils::computeDistance(config.distFunc, *embeddings, queryVector,
                neighbor, transaction);
            if (result.size() < ef || dist < result.top().distance) {
                if (result.size() == ef) {
                    result.pop();
                }
                result.push({neighbor, dist});
                candidates.push({neighbor, dist});
            }
        }
    }
    return gatherTopK(result, k);
}

std::vector<NodeWithDistance> HNSWIndex::gatherTopK(max_node_priority_queue_t& result,
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

common::offset_t HNSWIndex::searchUpperLayer(const float* queryVector,
    transaction::Transaction* transaction) const {
    if (upperGraph->getEntryPoint() == common::INVALID_OFFSET) {
        return common::INVALID_OFFSET;
    }
    double lastMinDist = std::numeric_limits<float>::max();
    auto minDist = HNSWIndexUtils::computeDistance(config.distFunc, *embeddings, queryVector,
        upperGraph->getEntryPoint(), transaction);
    KU_ASSERT(lastMinDist >= 0);
    KU_ASSERT(minDist >= 0);
    auto currentNode = upperGraph->getEntryPoint();
    while (minDist < lastMinDist) {
        lastMinDist = minDist;
        auto neighbors = upperGraph->getNeighbors(transaction, currentNode);
        for (const auto neighbor : neighbors) {
            const auto dist = HNSWIndexUtils::computeDistance(config.distFunc, *embeddings,
                queryVector, neighbor, transaction);
            if (dist < minDist) {
                minDist = dist;
                currentNode = neighbor;
            }
        }
    }
    return currentNode;
}

static uint64_t getMaxDegree(common::length_t degree) {
    return std::ceil(degree * 1.25);
}

InMemHNSWIndex::InMemHNSWIndex(main::ClientContext* context, NodeTable& table,
    common::column_id_t columnID, HNSWIndexConfig config)
    : HNSWIndex{std::move(config)} {
    auto& columnType = table.getColumn(columnID).getDataType();
    KU_ASSERT(columnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto extraInfo = columnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    EmbeddingTypeInfo typeInfo{extraInfo->getChildType().copy(), extraInfo->getNumElements()};
    embeddings = std::make_unique<InMemEmbeddings>(*context->getMemoryManager(),
        std::move(typeInfo), table.getNumTotalRows(context->getTx()), columnType);
    const auto numNodes = table.getNumTotalRows(context->getTx());
    HNSWGraphInfo lowerGraphInfo(numNodes, embeddings.get(), this->config.distFunc);
    lowerGraph = std::make_unique<InMemHNSWGraph>(context->getMemoryManager(), lowerGraphInfo,
        getMaxDegree(this->config.connectivityInLowerLayer), this->config.connectivityInLowerLayer,
        this->config.alpha);
    HNSWGraphInfo upperGraphInfo(numNodes, embeddings.get(), this->config.distFunc);
    upperGraph = std::make_unique<InMemHNSWGraph>(context->getMemoryManager(), upperGraphInfo,
        getMaxDegree(this->config.connectivityInUpperLayer), this->config.connectivityInUpperLayer,
        this->config.alpha);
}

// TODO(Guodong): Not consider the case that a embedding is NULL.
void InMemHNSWIndex::insert(common::offset_t offset, transaction::Transaction* transaction,
    VisitedState& visited) {
    const auto entryNode = insertToLowerLayer(offset, transaction, visited);
    const auto rand = randomEngine.nextRandomInteger(INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND);
    if (rand <= INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND * config.upperLayerNodePct) {
        insertToUpperLayer(offset, entryNode, transaction);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::shrink(transaction::Transaction* transaction) {
    for (auto i = 0u; i < upperGraph->getNumNodes(); i++) {
        upperGraph->shrink(transaction, i);
    }
    for (auto i = 0u; i < lowerGraph->getNumNodes(); i++) {
        lowerGraph->shrink(transaction, i);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
common::offset_t InMemHNSWIndex::insertToLowerLayer(common::offset_t offset,
    transaction::Transaction* transaction, VisitedState& visited) {
    const auto vector = embeddings->getEmebdding(offset, nullptr);
    const auto upperEntryPoint = searchUpperLayer(vector, transaction);
    if (upperEntryPoint == common::INVALID_OFFSET &&
        lowerGraph->getEntryPoint() == common::INVALID_OFFSET) {
        // The upper graph is empty.
        // The lower graph is also empty. No edges need to be created.
        lowerGraph->setEntryPoint(0);
        return common::INVALID_OFFSET;
    }
    if (lowerGraph->getEntryPoint() == common::INVALID_OFFSET) {
        lowerGraph->setEntryPoint(0);
    }
    common::offset_t lowerEntryPoint = upperEntryPoint;
    if (lowerEntryPoint == common::INVALID_OFFSET) {
        lowerEntryPoint = lowerGraph->getEntryPoint();
    }
    KU_ASSERT(lowerEntryPoint != common::INVALID_OFFSET);
    // Search from lower layer and create edges in lower layer.
    const auto nbrs = searchLowerLayer(vector, lowerEntryPoint,
        common::ku_dynamic_cast<InMemHNSWGraph*>(lowerGraph.get())->getMaxDegree(), config.efc,
        transaction, visited);
    for (auto [nodeOffset, distance] : nbrs) {
        lowerGraph->createDirectedEdge(transaction, offset, nodeOffset);
        lowerGraph->createDirectedEdge(transaction, nodeOffset, offset);
    }
    return upperEntryPoint;
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::insertToUpperLayer(common::offset_t offset, common::offset_t entryNode,
    transaction::Transaction* transaction) {
    if (entryNode == common::INVALID_OFFSET) {
        // The upper graph is empty.
        upperGraph->setEntryPoint(offset);
    } else {
        upperGraph->createDirectedEdge(transaction, offset, entryNode);
        upperGraph->createDirectedEdge(transaction, entryNode, offset);
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void InMemHNSWIndex::finalize(MemoryManager& mm,
    const HNSWIndexPartitionerSharedState& partitionerSharedState) {
    upperGraph->finalize(mm, *partitionerSharedState.upperPartitionerSharedState);
    lowerGraph->finalize(mm, *partitionerSharedState.lowerPartitionerSharedState);
}

OnDiskHNSWIndex::OnDiskHNSWIndex(main::ClientContext* context, NodeTable& nodeTable,
    common::column_id_t columnID, RelTable& upperRelTable, RelTable& lowerRelTable,
    HNSWIndexConfig config)
    : HNSWIndex{std::move(config)} {
    const auto& indexColumnType = nodeTable.getColumn(columnID).getDataType();
    KU_ASSERT(indexColumnType.getLogicalTypeID() == common::LogicalTypeID::ARRAY);
    const auto extraTypeInfo =
        indexColumnType.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>();
    EmbeddingTypeInfo indexColumnTypeInfo{extraTypeInfo->getChildType().copy(),
        extraTypeInfo->getNumElements()};
    embeddings = std::make_unique<OnDiskEmbeddings>(context->getMemoryManager(),
        std::move(indexColumnTypeInfo), nodeTable, columnID);
    const auto numNodes = nodeTable.getNumTotalRows(context->getTx());
    HNSWGraphInfo lowerGraphInfo(numNodes, embeddings.get(), config.distFunc);
    lowerGraph =
        std::make_unique<OnDiskHNSWGraph>(context, lowerGraphInfo, nodeTable, lowerRelTable);
    HNSWGraphInfo upperGraphInfo(numNodes, embeddings.get(), config.distFunc);
    upperGraph =
        std::make_unique<OnDiskHNSWGraph>(context, upperGraphInfo, nodeTable, upperRelTable);
}

std::vector<NodeWithDistance> OnDiskHNSWIndex::search(const std::vector<float>& queryVector,
    common::length_t k, const QueryHNSWConfig& config, transaction::Transaction* transaction,
    VisitedState& visited) const {
    auto entryPoint = searchUpperLayer(&queryVector[0], transaction);
    if (entryPoint == common::INVALID_OFFSET) {
        entryPoint = lowerGraph->getEntryPoint();
    }
    KU_ASSERT(entryPoint != common::INVALID_OFFSET);
    return searchLowerLayer(&queryVector[0], entryPoint, k, config.efs, transaction, visited);
}
} // namespace storage
} // namespace kuzu
