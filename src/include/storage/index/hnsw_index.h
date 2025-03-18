#pragma once

#include <queue>

#include "common/random_engine.h"
#include "graph/on_disk_graph.h"
#include "storage/index/hnsw_graph.h"

namespace kuzu {
namespace function {
struct HNSWSearchState;
struct QueryHNSWLocalState;
} // namespace function
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
    common::offset_t compareAndSwapEntryPoint(common::offset_t offset) {
        common::offset_t oldOffset = common::INVALID_OFFSET;
        entryPoint.compare_exchange_strong(oldOffset, offset);
        return oldOffset;
    }
    common::offset_t getEntryPoint() const { return entryPoint.load(); }

    void insert(common::offset_t offset, common::offset_t entryPoint_, VisitedState& visited);
    common::offset_t searchNN(common::offset_t node, common::offset_t entryNode) const;
    void finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        const processor::PartitionerSharedState& partitionerSharedState) const;

private:
    std::vector<NodeWithDistance> searchKNN(const float* queryVector, common::offset_t entryNode,
        common::length_t k, uint64_t configuredEf, VisitedState& visited) const;
    static void shrinkForNode(const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph,
        common::offset_t nodeOffset, common::length_t numNbrs);

    void insertRel(common::offset_t srcNode, common::offset_t dstNode);

private:
    std::atomic<common::offset_t> entryPoint;
    std::unique_ptr<InMemHNSWGraph> graph;
    InMemHNSWLayerInfo info;
};

class InMemHNSWIndex final : public HNSWIndex {
public:
    InMemHNSWIndex(const main::ClientContext* context, NodeTable& table,
        common::column_id_t columnID, HNSWIndexConfig config);

    common::offset_t getUpperEntryPoint() const override { return upperLayer->getEntryPoint(); }
    common::offset_t getLowerEntryPoint() const override { return lowerLayer->getEntryPoint(); }

    // Note that the input is only `offset`, as we assume embeddings are already cached in memory.
    bool insert(common::offset_t offset, VisitedState& upperVisited, VisitedState& lowerVisited);
    void finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        const HNSWIndexPartitionerSharedState& partitionerSharedState);

    void resetEmbeddings() { embeddings.reset(); }

private:
    static constexpr int64_t INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND = 100;

    std::unique_ptr<InMemHNSWLayer> upperLayer;
    std::unique_ptr<InMemHNSWLayer> lowerLayer;
    std::unique_ptr<InMemEmbeddings> embeddings;

    common::RandomEngine randomEngine;
};

enum class SearchType : uint8_t {
    BLIND_TWO_HOP = 0,
    DIRECTED_TWO_HOP = 1,
    ONE_HOP_FILTERED = 2,
    UNFILTERED = 3,
};

struct HNSWSearchState {
    VisitedState visited;
    OnDiskEmbeddingScanState embeddingScanState;
    uint64_t k;
    QueryHNSWConfig config;
    uint64_t ef;
    common::SemiMask* semiMask;

    SearchType searchType;
    std::unique_ptr<graph::NbrScanState> nbrScanState;
    std::unique_ptr<graph::NbrScanState> secondHopNbrScanState;

    HNSWSearchState(const transaction::Transaction* transaction, MemoryManager* mm,
        NodeTable& nodeTable, common::column_id_t columnID, common::offset_t numNodes, uint64_t k,
        QueryHNSWConfig config)
        : visited{numNodes}, embeddingScanState{transaction, mm, nodeTable, columnID}, k{k},
          config{config}, semiMask{nullptr}, searchType{SearchType::UNFILTERED},
          nbrScanState{nullptr}, secondHopNbrScanState{nullptr} {
        ef = std::max(k, static_cast<uint64_t>(config.efs));
    }

    bool isMasked(common::offset_t offset) const {
        return !hasMask() || semiMask->isMasked(offset);
    }
    bool hasMask() const { return semiMask != nullptr; }
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
        const std::vector<float>& queryVector, HNSWSearchState& searchState) const;

    common::offset_t searchNNInUpperLayer(transaction::Transaction* transaction,
        const float* queryVector, NodeTableScanState& embeddingScanState) const;
    std::vector<NodeWithDistance> searchKNNInLowerLayer(transaction::Transaction* transaction,
        const float* queryVector, common::offset_t entryNode, HNSWSearchState& searchState) const;

    void initLowerLayerSearchState(transaction::Transaction* transaction,
        HNSWSearchState& searchState) const;
    void oneHopSearch(transaction::Transaction* transaction, const float* queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void directedTwoHopFilteredSearch(transaction::Transaction* transaction,
        const float* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void blindTwoHopFilteredSearch(transaction::Transaction* transaction, const float* queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    void processSecondHopCandidates(transaction::Transaction* transaction, const float* queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        const std::vector<common::offset_t>& candidateOffsets) const;
    void processSecondHopCandidates(transaction::Transaction* transaction, const float* queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        min_node_priority_queue_t& candidatesQueue) const;

    min_node_priority_queue_t collectFirstHopNbrsDirected(transaction::Transaction* transaction,
        const float* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    common::offset_vec_t collectFirstHopNbrsBlind(transaction::Transaction* transaction,
        const float* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    // Return false if we've hit Ml limit.
    bool searchOverSecondHopNbrs(transaction::Transaction* transaction, const float* queryVector,
        uint64_t ef, HNSWSearchState& searchState, common::offset_t cand, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    SearchType getFilteredSearchType(transaction::Transaction* transaction,
        const HNSWSearchState& searchState) const;
    void initSearchCandidates(transaction::Transaction* transaction, const float* queryVector,
        HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
        max_node_priority_queue_t& results) const;

private:
    static constexpr double BLIND_SEARCH_UP_SEL_THRESHOLD = 0.08;
    static constexpr double DIRECTED_SEARCH_UP_SEL_THRESHOLD = 0.4;
    static constexpr uint64_t FILTERED_SEARCH_INITIAL_CANDIDATES = 10;

    common::table_id_t nodeTableID;
    catalog::RelTableCatalogEntry* upperRelTableEntry;
    catalog::RelTableCatalogEntry* lowerRelTableEntry;

    // The search starts in the upper layer to find the closest node, which serves as the entry
    // point for the lower layer search. If the upper layer does not return a valid entry point,
    // the search falls back to the defaultLowerEntryPoint in the lower layer.
    std::atomic<common::offset_t> defaultUpperEntryPoint;
    std::atomic<common::offset_t> defaultLowerEntryPoint;
    std::unique_ptr<graph::OnDiskGraph> upperGraph;
    std::unique_ptr<graph::OnDiskGraph> lowerGraph;
    std::unique_ptr<OnDiskEmbeddings> embeddings;
};

} // namespace storage
} // namespace kuzu
