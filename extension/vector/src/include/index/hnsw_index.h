#pragma once

#include <queue>

#include "common/random_engine.h"
#include "graph/on_disk_graph.h"
#include "index/hnsw_config.h"
#include "index/hnsw_graph.h"
#include "index/hnsw_index_utils.h"
#include "storage/index/index.h"

namespace kuzu {
namespace catalog {
class IndexCatalogEntry;
}
namespace function {
struct HNSWSearchState;
struct QueryHNSWLocalState;
} // namespace function
namespace processor {
struct PartitionerSharedState;
} // namespace processor
namespace storage {
class NodeTable;
} // namespace storage

namespace vector_extension {

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

    explicit HNSWIndexPartitionerSharedState(storage::MemoryManager& mm)
        : lowerPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)},
          upperPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)} {}

    void setTables(storage::NodeTable* nodeTable, storage::RelTable* relTable);

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

struct HNSWStorageInfo final : storage::IndexStorageInfo {
    common::table_id_t upperRelTableID;
    common::table_id_t lowerRelTableID;
    common::offset_t upperEntryPoint;
    common::offset_t lowerEntryPoint;

    HNSWStorageInfo()
        : upperRelTableID{common::INVALID_TABLE_ID}, lowerRelTableID{common::INVALID_TABLE_ID},
          upperEntryPoint{common::INVALID_OFFSET}, lowerEntryPoint{common::INVALID_OFFSET} {}
    HNSWStorageInfo(const common::table_id_t& upperRelTableID,
        const common::table_id_t& lowerRelTableID, common::offset_t upperEntryPoint,
        common::offset_t lowerEntryPoint)
        : upperRelTableID{upperRelTableID}, lowerRelTableID{lowerRelTableID},
          upperEntryPoint{upperEntryPoint}, lowerEntryPoint{lowerEntryPoint} {}

    std::shared_ptr<common::BufferedSerializer> serialize() const override;

    static std::unique_ptr<IndexStorageInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);
};

class HNSWIndex : public storage::Index {
public:
    explicit HNSWIndex(storage::IndexInfo indexInfo,
        std::unique_ptr<storage::IndexStorageInfo> storageInfo, HNSWIndexConfig config,
        common::ArrayTypeInfo typeInfo)
        : Index{std::move(indexInfo), std::move(storageInfo)}, config{std::move(config)},
          typeInfo{std::move(typeInfo)} {
        metricFunc =
            HNSWIndexUtils::getMetricsFunction(this->config.metric, this->typeInfo.getChildType());
    }
    ~HNSWIndex() override = default;

    common::LogicalType getElementType() const { return typeInfo.getChildType().copy(); }

    static std::vector<NodeWithDistance> popTopK(max_node_priority_queue_t& result,
        common::length_t k);

protected:
    static constexpr int64_t INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND = 100;

    HNSWIndexConfig config;
    common::ArrayTypeInfo typeInfo;
    metric_func_t metricFunc;
    common::RandomEngine randomEngine;
};

struct InMemHNSWLayerInfo {
    common::offset_t numNodes;
    InMemEmbeddings* embeddings;
    metric_func_t metricFunc;
    // The degree threshold of a node that will start to trigger shrinking during insertions. Thus,
    // it is also the max degree of a node in the graph before shrinking.
    int64_t degreeThresholdToShrink;
    // The max degree of a node in the graph after shrinking.
    int64_t maxDegree;
    double alpha;
    int64_t efc;

    InMemHNSWLayerInfo(common::offset_t numNodes, InMemEmbeddings* embeddings,
        metric_func_t metricFunc, int64_t degreeThresholdToShrink, int64_t maxDegree, double alpha,
        int64_t efc)
        : numNodes{numNodes}, embeddings{embeddings}, metricFunc{std::move(metricFunc)},
          degreeThresholdToShrink{degreeThresholdToShrink}, maxDegree{maxDegree}, alpha{alpha},
          efc{efc} {}

    uint64_t getDimension() const { return embeddings->getDimension(); }
};

class InMemHNSWLayer {
public:
    explicit InMemHNSWLayer(storage::MemoryManager* mm, InMemHNSWLayerInfo info);
    common::offset_t compareAndSwapEntryPoint(common::offset_t offset) {
        common::offset_t oldOffset = common::INVALID_OFFSET;
        entryPoint.compare_exchange_strong(oldOffset, offset);
        return oldOffset;
    }
    common::offset_t getEntryPoint() const { return entryPoint.load(); }

    void insert(common::offset_t offset, common::offset_t entryPoint_, VisitedState& visited);
    common::offset_t searchNN(common::offset_t node, common::offset_t entryNode) const;
    void finalize(storage::MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        const processor::PartitionerSharedState& partitionerSharedState) const;

private:
    std::vector<NodeWithDistance> searchKNN(const void* queryVector, common::offset_t entryNode,
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
    InMemHNSWIndex(const main::ClientContext* context, storage::IndexInfo indexInfo,
        std::unique_ptr<storage::IndexStorageInfo> storageInfo, storage::NodeTable& table,
        common::column_id_t columnID, HNSWIndexConfig config);

    common::offset_t getUpperEntryPoint() const { return upperLayer->getEntryPoint(); }
    common::offset_t getLowerEntryPoint() const { return lowerLayer->getEntryPoint(); }

    std::unique_ptr<InsertState> initInsertState(const transaction::Transaction*,
        storage::MemoryManager*, storage::visible_func) override {
        KU_UNREACHABLE;
    }
    void insert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, InsertState&) override {
        KU_UNREACHABLE;
    }
    // Note that the input is only `offset`, as we assume embeddings are already cached in memory.
    bool insert(common::offset_t offset, VisitedState& upperVisited, VisitedState& lowerVisited);
    void finalize(storage::MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        const HNSWIndexPartitionerSharedState& partitionerSharedState);

    void resetEmbeddings() { embeddings.reset(); }

private:
    std::unique_ptr<InMemHNSWLayer> upperLayer;
    std::unique_ptr<InMemHNSWLayer> lowerLayer;
    std::unique_ptr<InMemEmbeddings> embeddings;
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
    std::unique_ptr<graph::OnDiskGraph> upperGraph;
    std::unique_ptr<graph::OnDiskGraph> lowerGraph;

    SearchType searchType;
    std::unique_ptr<graph::NbrScanState> nbrScanState;
    std::unique_ptr<graph::NbrScanState> secondHopNbrScanState;

    HNSWSearchState(main::ClientContext* context, catalog::TableCatalogEntry* nodeTableEntry,
        catalog::TableCatalogEntry* upperRelTableEntry,
        catalog::TableCatalogEntry* lowerRelTableEntry, storage::NodeTable& nodeTable,
        common::column_id_t columnID, common::offset_t numNodes, uint64_t k,
        QueryHNSWConfig config);

    bool isMasked(common::offset_t offset) const {
        return !hasMask() || semiMask->isMasked(offset);
    }
    bool hasMask() const { return semiMask != nullptr; }
};

class OnDiskHNSWIndex final : public HNSWIndex {
public:
    OnDiskHNSWIndex(main::ClientContext* context, storage::IndexInfo indexInfo,
        std::unique_ptr<storage::IndexStorageInfo> storageInfo,
        catalog::NodeTableCatalogEntry* nodeTableEntry,
        catalog::RelGroupCatalogEntry* upperRelTableEntry,
        catalog::RelGroupCatalogEntry* lowerRelTableEntry, HNSWIndexConfig config);

    std::vector<NodeWithDistance> search(transaction::Transaction* transaction,
        const void* queryVector, HNSWSearchState& searchState) const;

    static std::unique_ptr<Index> load(main::ClientContext* context,
        storage::StorageManager* storageManager, storage::IndexInfo indexInfo,
        std::span<uint8_t> storageInfoBuffer);
    std::unique_ptr<InsertState> initInsertState(const transaction::Transaction*,
        storage::MemoryManager*, storage::visible_func) override {
        KU_UNREACHABLE;
    }
    void insert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, Index::InsertState&) override {
        KU_UNREACHABLE;
    }

    static storage::IndexType getIndexType() {
        static const storage::IndexType HNSW_INDEX_TYPE{"HNSW",
            storage::IndexConstraintType::SECONDARY_NON_UNIQUE,
            storage::IndexDefinitionType::EXTENSION, load};
        return HNSW_INDEX_TYPE;
    }

private:
    common::offset_t searchNNInUpperLayer(transaction::Transaction* transaction,
        const void* queryVector, HNSWSearchState& searchState) const;
    std::vector<NodeWithDistance> searchKNNInLowerLayer(transaction::Transaction* transaction,
        const void* queryVector, common::offset_t entryNode, HNSWSearchState& searchState) const;

    void initLowerLayerSearchState(transaction::Transaction* transaction,
        HNSWSearchState& searchState) const;
    void oneHopSearch(transaction::Transaction* transaction, const void* queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void directedTwoHopFilteredSearch(transaction::Transaction* transaction,
        const void* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void blindTwoHopFilteredSearch(transaction::Transaction* transaction, const void* queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    void processSecondHopCandidates(transaction::Transaction* transaction, const void* queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        const std::vector<common::offset_t>& candidateOffsets) const;
    void processSecondHopCandidates(transaction::Transaction* transaction, const void* queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        min_node_priority_queue_t& candidatesQueue) const;

    min_node_priority_queue_t collectFirstHopNbrsDirected(transaction::Transaction* transaction,
        const void* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    common::offset_vec_t collectFirstHopNbrsBlind(transaction::Transaction* transaction,
        const void* queryVector, graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    // Return false if we've hit Ml limit.
    bool searchOverSecondHopNbrs(transaction::Transaction* transaction, const void* queryVector,
        uint64_t ef, HNSWSearchState& searchState, common::offset_t cand, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    void initSearchCandidates(transaction::Transaction* transaction, const void* queryVector,
        HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
        max_node_priority_queue_t& results) const;

    static SearchType getFilteredSearchType(transaction::Transaction* transaction,
        const HNSWSearchState& searchState);

private:
    static constexpr uint64_t FILTERED_SEARCH_INITIAL_CANDIDATES = 10;

    storage::NodeTable& nodeTable;
    catalog::RelGroupCatalogEntry* upperRelTableEntry;
    catalog::RelGroupCatalogEntry* lowerRelTableEntry;

    // std::unique_ptr<graph::OnDiskGraph> upperGraph;
    // std::unique_ptr<graph::OnDiskGraph> lowerGraph;
    storage::RelTable* upperRelTable;
    storage::RelTable* lowerRelTable;
    std::unique_ptr<OnDiskEmbeddings> embeddings;
};

} // namespace vector_extension
} // namespace kuzu
