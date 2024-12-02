#pragma once

#include <queue>

#include "common/random_engine.h"
#include "graph/on_disk_graph.h"
#include "storage/index/hnsw_graph.h"

namespace kuzu {
namespace function {
struct QueryHNSWLocalState;
}
namespace processor {
struct PartitionerSharedState;
}
namespace storage {
class NodeTable;

struct MinNodePriorityQueueComparator {
    bool operator()(const NodeWithDistance& l, const NodeWithDistance& r) const {
        return l.distance > r.distance;
    }
};
struct MaxNodePriorityQueueComparator {
    bool operator()(const NodeWithDistance& l, const NodeWithDistance& r) const {
        return l.distance < r.distance;
    }
};

using min_node_priority_queue_t = std::priority_queue<NodeWithDistance,
    std::vector<NodeWithDistance>, MinNodePriorityQueueComparator>;
using max_node_priority_queue_t = std::priority_queue<NodeWithDistance,
    std::vector<NodeWithDistance>, MaxNodePriorityQueueComparator>;

struct HNSWIndexPartitionerSharedState {
    std::shared_ptr<processor::PartitionerSharedState> lowerPartitionerSharedState;
    std::shared_ptr<processor::PartitionerSharedState> upperPartitionerSharedState;

    explicit HNSWIndexPartitionerSharedState(MemoryManager& mm)
        : lowerPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)},
          upperPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)} {}

    void setTables(NodeTable* nodeTable, RelTable* relTable);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void initialize(const common::logical_type_vec_t& columnTypes,
        const main::ClientContext* clientContext) {
        lowerPartitionerSharedState->initialize(columnTypes, 1 /*numPartitioners*/, clientContext);
        upperPartitionerSharedState->initialize(columnTypes, 1 /*numPartitioners*/, clientContext);
    }
};

struct VisitedState {
    common::offset_t size;
    std::unique_ptr<uint8_t[]> visited;

    explicit VisitedState(common::offset_t size) : size{size} {
        visited = std::make_unique<uint8_t[]>(size);
        memset(visited.get(), 0, size);
    }

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void reset() { memset(visited.get(), 0, size); }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void add(common::offset_t offset) { visited[offset] = 1; }
    bool contains(common::offset_t offset) const { return visited[offset]; }
};

class HNSWIndex {
public:
    explicit HNSWIndex(HNSWIndexConfig config) : config{std::move(config)} {}
    virtual ~HNSWIndex() = default;

    virtual common::offset_t getUpperEntryPoint() const = 0;
    virtual common::offset_t getLowerEntryPoint() const = 0;

    static std::vector<NodeWithDistance> popTopK(max_node_priority_queue_t& result,
        common::length_t k);

protected:
    HNSWIndexConfig config;
};

struct InMemHNSWLayerInfo {
    common::offset_t numNodes;
    InMemEmbeddings* embeddings;
    DistFuncType distFunc;
    // The degree threshold of a node that will start to trigger shrinking during insertions. Thus,
    // it is also the max degree of a node in the graph before shrinking.
    int64_t degreeThresholdToShrink;
    // The max degree of a node in the graph after shrinking.
    int64_t maxDegree;
    double alpha;
    int64_t efc;

    InMemHNSWLayerInfo(common::offset_t numNodes, InMemEmbeddings* embeddings,
        DistFuncType distFunc, int64_t degreeThresholdToShrink, int64_t maxDegree, double alpha,
        int64_t efc)
        : numNodes{numNodes}, embeddings{embeddings}, distFunc{distFunc},
          degreeThresholdToShrink{degreeThresholdToShrink}, maxDegree{maxDegree}, alpha{alpha},
          efc{efc} {}
};

class InMemHNSWLayer {
public:
    explicit InMemHNSWLayer(MemoryManager* mm, InMemHNSWLayerInfo info);
    void setEntryPoint(common::offset_t offset) { entryPoint.store(offset); }
    common::offset_t getEntryPoint() const { return entryPoint.load(); }

    void insert(transaction::Transaction* transaction, common::offset_t offset,
        common::offset_t entryPoint_, VisitedState& visited);
    common::offset_t searchNN(transaction::Transaction* transaction, common::offset_t node,
        common::offset_t entryNode) const;
    void shrink(transaction::Transaction* transaction);
    void finalize(MemoryManager& mm,
        const processor::PartitionerSharedState& partitionerSharedState) const;

private:
    std::vector<NodeWithDistance> searchKNN(transaction::Transaction* transaction,
        const float* queryVector, common::offset_t entryNode, common::length_t k,
        uint64_t configuredEf, VisitedState& visited) const;
    static void shrinkForNode(transaction::Transaction* transaction, const InMemHNSWLayerInfo& info,
        InMemHNSWGraph* graph, common::offset_t nodeOffset, common::length_t numNbrs);

    void insertRel(transaction::Transaction* transaction, common::offset_t srcNode,
        common::offset_t dstNode);

private:
    std::atomic<common::offset_t> entryPoint;
    std::unique_ptr<InMemHNSWGraph> graph;
    InMemHNSWLayerInfo info;
};

class InMemHNSWIndex final : public HNSWIndex {
public:
    InMemHNSWIndex(main::ClientContext* context, NodeTable& table, common::column_id_t columnID,
        HNSWIndexConfig config);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void initialize(main::ClientContext* context, NodeTable& table, common::column_id_t columnID) {
        embeddings->initialize(context, table, columnID);
    }

    common::offset_t getUpperEntryPoint() const override { return upperLayer->getEntryPoint(); }
    common::offset_t getLowerEntryPoint() const override { return lowerLayer->getEntryPoint(); }

    // Note that the input is only `offset`, as we assume embeddings are already cached in memory.
    void insert(common::offset_t offset, transaction::Transaction* transaction,
        VisitedState& upperVisited, VisitedState& lowerVisited);
    void shrink(transaction::Transaction* transaction);
    void finalize(MemoryManager& mm, const HNSWIndexPartitionerSharedState& partitionerSharedState);

private:
    static constexpr int64_t INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND = 100;

    std::unique_ptr<InMemHNSWLayer> upperLayer;
    std::unique_ptr<InMemHNSWLayer> lowerLayer;
    std::unique_ptr<InMemEmbeddings> embeddings;

    common::RandomEngine randomEngine;
};

class OnDiskHNSWIndex final : public HNSWIndex {
public:
    OnDiskHNSWIndex(main::ClientContext* context, catalog::NodeTableCatalogEntry* nodeTableEntry,
        common::column_id_t columnID, catalog::RelTableCatalogEntry* upperRelTableEntry,
        catalog::RelTableCatalogEntry* lowerRelTableEntry, HNSWIndexConfig config);

    void setDefaultUpperEntryPoint(common::offset_t offset) {
        defaultUpperEntryPoint.store(offset);
    }
    void setDefaultLowerEntryPoint(common::offset_t offset) {
        defaultLowerEntryPoint.store(offset);
    }
    common::offset_t getUpperEntryPoint() const override { return defaultUpperEntryPoint.load(); }
    common::offset_t getLowerEntryPoint() const override { return defaultLowerEntryPoint.load(); }

    std::vector<NodeWithDistance> search(transaction::Transaction* transaction,
        const std::vector<float>& queryVector, common::length_t k, const QueryHNSWConfig& config,
        VisitedState& visited, NodeTableScanState& embeddingScanState) const;

private:
    common::offset_t searchNNInUpperLayer(transaction::Transaction* transaction,
        const float* queryVector, NodeTableScanState& embeddingScanState) const;
    std::vector<NodeWithDistance> searchKNNInLowerLayer(transaction::Transaction* transaction,
        const float* queryVector, common::offset_t entryNode, common::length_t k,
        uint64_t configuredEf, VisitedState& visited, NodeTableScanState& embeddingScanState) const;

private:
    common::table_id_t nodeTableID;
    catalog::RelTableCatalogEntry* upperRelTableEntry;
    catalog::RelTableCatalogEntry* lowerRelTableEntry;

    // The search starts in the upper layer to find the closest node, which serves as the entry
    // point for the lower layer search. If the upper layer does not return a valid entry point, the
    // search falls back to the defaultLowerEntryPoint in the lower layer.
    std::atomic<common::offset_t> defaultUpperEntryPoint;
    std::atomic<common::offset_t> defaultLowerEntryPoint;
    std::unique_ptr<graph::OnDiskGraph> upperGraph;
    std::unique_ptr<graph::OnDiskGraph> lowerGraph;
    std::unique_ptr<OnDiskEmbeddings> embeddings;
};

} // namespace storage
} // namespace kuzu
