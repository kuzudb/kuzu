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
namespace processor {
struct CopyPartitionerSharedState;
} // namespace processor
namespace storage {
class NodeTable;
} // namespace storage

namespace vector_extension {

struct HNSWIndexPartitionerSharedState;
struct CreateInMemHNSWLocalState;

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
    common::offset_t numCheckpointedNodes;

    HNSWStorageInfo()
        : upperRelTableID{common::INVALID_TABLE_ID}, lowerRelTableID{common::INVALID_TABLE_ID},
          upperEntryPoint{common::INVALID_OFFSET}, lowerEntryPoint{common::INVALID_OFFSET},
          numCheckpointedNodes{0} {}
    HNSWStorageInfo(common::table_id_t upperRelTableID, common::table_id_t lowerRelTableID,
        common::offset_t upperEntryPoint, common::offset_t lowerEntryPoint,
        common::offset_t numCheckpointedNodes)
        : upperRelTableID{upperRelTableID}, lowerRelTableID{lowerRelTableID},
          upperEntryPoint{upperEntryPoint}, lowerEntryPoint{lowerEntryPoint},
          numCheckpointedNodes{numCheckpointedNodes} {}

    std::shared_ptr<common::BufferWriter> serialize() const override;

    static std::unique_ptr<IndexStorageInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);
};

class HNSWIndex : public storage::Index {
public:
    static constexpr int64_t INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND = 100;

    HNSWIndex(storage::IndexInfo indexInfo, std::unique_ptr<storage::IndexStorageInfo> storageInfo,
        HNSWIndexConfig config, common::ArrayTypeInfo typeInfo)
        : Index{std::move(indexInfo), std::move(storageInfo)}, config{std::move(config)},
          typeInfo{std::move(typeInfo)} {
        metricFunc =
            HNSWIndexUtils::getMetricsFunction(this->config.metric, this->typeInfo.getChildType());
    }
    ~HNSWIndex() override = default;

    void commitInsert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, InsertState&) override {
        // DO NOTHING.
        // For HNSW index, insertions are handled when the new tuples are inserted into the base
        // table being indexed.
    }

    common::LogicalType getElementType() const { return typeInfo.getChildType().copy(); }

    static std::vector<NodeWithDistance> popTopK(max_node_priority_queue_t& result,
        common::length_t k);

    std::unique_ptr<UpdateState> initUpdateState(main::ClientContext* /*context*/,
        common::column_id_t /*columnID*/, storage::visible_func /*isVisible*/) override {
        throw common::RuntimeException{"Cannot set property vec in table embeddings because it is "
                                       "used in one or more indexes. Try delete and then insert."};
    }

    std::unique_ptr<DeleteState> initDeleteState(const transaction::Transaction* /*transaction*/,
        storage::MemoryManager* /*mm*/, storage::visible_func /*isVisible*/) override {
        return std::make_unique<DeleteState>();
    }
    void delete_(transaction::Transaction* /*transaction*/,
        const common::ValueVector& /*nodeIDVector*/, DeleteState& /*deleteState*/) override {
        // DO NOTHING.
    }

    static int64_t getDegreeThresholdToShrink(int64_t degree);
    static int64_t getMaximumSupportedMl();

protected:
    HNSWIndexConfig config;
    common::ArrayTypeInfo typeInfo;
    metric_func_t metricFunc;
    common::RandomEngine randomEngine;
};

struct InMemHNSWLayerInfo {
    common::offset_t numNodes;
    HNSWIndexEmbeddings* embeddings;
    metric_func_t metricFunc;
    // The degree threshold of a node that will start to trigger shrinking during insertions. Thus,
    // it is also the max degree of a node in the graph before shrinking.
    int64_t degreeThresholdToShrink;
    // The max degree of a node in the graph after shrinking.
    int64_t maxDegree;
    double alpha;
    int64_t efc;
    const NodeToHNSWGraphOffsetMap& offsetMap;

    InMemHNSWLayerInfo(common::offset_t numNodes, HNSWIndexEmbeddings* embeddings,
        metric_func_t metricFunc, int64_t degreeThresholdToShrink, int64_t maxDegree, double alpha,
        int64_t efc, const NodeToHNSWGraphOffsetMap& offsetMap)
        : numNodes{numNodes}, embeddings{embeddings}, metricFunc{std::move(metricFunc)},
          degreeThresholdToShrink{degreeThresholdToShrink}, maxDegree{maxDegree}, alpha{alpha},
          efc{efc}, offsetMap(offsetMap) {}

    uint64_t getDimension() const { return embeddings->getDimension(); }
    EmbeddingHandle getEmbedding(common::offset_t offsetInGraph,
        GetEmbeddingsScanState& scanState) const {
        KU_ASSERT(offsetInGraph < numNodes);
        return embeddings->getEmbedding(offsetMap.graphToNodeOffset(offsetInGraph), scanState);
    }
    std::vector<EmbeddingHandle> getEmbeddings(std::span<const common::offset_t> offsetsInGraph,
        GetEmbeddingsScanState& scanState) const;
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
    common::offset_t getNumNodes() const { return info.numNodes; }

    void insert(common::offset_t offset, common::offset_t entryPoint_, VisitedState& visited,
        GetEmbeddingsScanState& scanState);
    common::offset_t searchNN(const EmbeddingHandle& queryVector, common::offset_t entryNode,
        GetEmbeddingsScanState& scanState) const;
    void finalizeNodeGroup(common::node_group_idx_t nodeGroupIdx, common::offset_t numNodesInTable,
        const NodeToHNSWGraphOffsetMap& selectedNodesMap, GetEmbeddingsScanState& scanState) const;

    std::unique_ptr<InMemHNSWGraph> moveGraph() { return std::move(graph); }

private:
    std::vector<NodeWithDistance> searchKNN(const EmbeddingHandle& queryVector,
        common::offset_t entryNode, common::length_t k, uint64_t configuredEf,
        VisitedState& visited, GetEmbeddingsScanState& scanState) const;
    static void shrinkForNode(const InMemHNSWLayerInfo& info, InMemHNSWGraph* graph,
        common::offset_t nodeOffset, common::length_t numNbrs, GetEmbeddingsScanState& scanState);

    void insertRel(common::offset_t srcNode, common::offset_t dstNode,
        GetEmbeddingsScanState& scanState);

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

    common::offset_t upperGraphOffsetToNodeOffset(common::offset_t upperOffset) const {
        return upperOffset == common::INVALID_OFFSET ?
                   common::INVALID_OFFSET :
                   upperGraphSelectionMap->graphToNodeOffset(upperOffset);
    }
    common::offset_t getUpperEntryPoint() const {
        return upperGraphOffsetToNodeOffset(upperLayer->getEntryPoint());
    }
    common::offset_t getLowerEntryPoint() const { return lowerLayer->getEntryPoint(); }
    common::offset_t getNumUpperLayerNodes() const {
        return upperGraphSelectionMap->getNumNodesInGraph();
    }

    std::unique_ptr<InsertState> initInsertState(main::ClientContext*,
        storage::visible_func) override {
        KU_UNREACHABLE;
    }
    void insert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, InsertState&) override {
        KU_UNREACHABLE;
    }
    // Note that the input is only `offset`, as we assume embeddings are already cached in memory.
    bool insert(common::offset_t offset, CreateInMemHNSWLocalState* localState);
    void finalizeNodeGroup(common::node_group_idx_t nodeGroupIdx);

    void moveToPartitionState(HNSWIndexPartitionerSharedState& partitionState);

    std::unique_ptr<GetEmbeddingsScanState> constructEmbeddingsScanState() const {
        return embeddings->constructScanState();
    }

private:
    std::unique_ptr<InMemHNSWLayer> upperLayer;
    std::unique_ptr<InMemHNSWLayer> lowerLayer;
    std::unique_ptr<HNSWIndexEmbeddings> embeddings;

    std::unique_ptr<NodeToHNSWGraphOffsetMap> lowerGraphSelectionMap; // this mapping is trivial
    std::unique_ptr<NodeToHNSWGraphOffsetMap> upperGraphSelectionMap;
    std::unique_ptr<common::NullMask> upperLayerSelectionMask;
};

enum class SearchType : uint8_t {
    BLIND_TWO_HOP = 0,
    DIRECTED_TWO_HOP = 1,
    ONE_HOP_FILTERED = 2,
    UNFILTERED = 3,
};

struct HNSWSearchState {
    VisitedState visited;
    std::unique_ptr<HNSWIndexEmbeddings> embeddings;
    OnDiskEmbeddingScanState embeddingScanState;
    uint64_t k;
    QueryHNSWConfig config;
    uint64_t ef;
    common::SemiMask* semiMask;
    catalog::TableCatalogEntry* upperRelTableEntry;
    catalog::TableCatalogEntry* lowerRelTableEntry;
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
    struct HNSWInsertState final : InsertState {
        // State for searching neighbors and reading vectors in the HNSW graph.
        HNSWSearchState searchState;
        // State for inserting rels.
        common::DataChunk srcNodeIDChunk;
        common::DataChunk insertChunk;
        std::unique_ptr<storage::RelTableInsertState> relInsertState;
        // State for detaching delete.
        std::unique_ptr<storage::RelTableDeleteState> relDeleteState;
        // Nodes to shrink at the end of insertions.
        std::unordered_set<common::offset_t> upperNodesToShrink;
        std::unordered_set<common::offset_t> lowerNodesToShrink;

        HNSWInsertState(main::ClientContext* context, catalog::TableCatalogEntry* nodeTableEntry,
            catalog::TableCatalogEntry* upperRelTableEntry,
            catalog::TableCatalogEntry* lowerRelTableEntry, storage::NodeTable& nodeTable,
            common::column_id_t columnID, uint64_t degree);
    };

    OnDiskHNSWIndex(const main::ClientContext* context, storage::IndexInfo indexInfo,
        std::unique_ptr<storage::IndexStorageInfo> storageInfo, HNSWIndexConfig config);

    std::vector<NodeWithDistance> search(transaction::Transaction* transaction,
        const EmbeddingHandle& queryVector, HNSWSearchState& searchState) const;

    static std::unique_ptr<Index> load(main::ClientContext* context,
        storage::StorageManager* storageManager, storage::IndexInfo indexInfo,
        std::span<uint8_t> storageInfoBuffer);
    std::unique_ptr<InsertState> initInsertState(main::ClientContext* context,
        storage::visible_func) override;
    bool needCommitInsert() const override { return true; }
    void commitInsert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, InsertState&) override;

    static storage::IndexType getIndexType() {
        static const storage::IndexType HNSW_INDEX_TYPE{"HNSW",
            storage::IndexConstraintType::SECONDARY_NON_UNIQUE,
            storage::IndexDefinitionType::EXTENSION, load};
        return HNSW_INDEX_TYPE;
    }

    void finalize(main::ClientContext*) override;
    void checkpoint(main::ClientContext* context, storage::PageAllocator& pageAllocator) override;

private:
    common::offset_t searchNNInUpperLayer(const EmbeddingHandle& queryVector,
        HNSWSearchState& searchState) const;
    std::vector<NodeWithDistance> searchKNNInLayer(transaction::Transaction* transaction,
        const EmbeddingHandle& queryVector, common::offset_t entryNode,
        HNSWSearchState& searchState, bool isUpperLayer) const;
    std::vector<NodeWithDistance> searchFromCheckpointed(transaction::Transaction* transaction,
        const EmbeddingHandle& queryVector, HNSWSearchState& searchState) const;
    void searchFromUnCheckpointed(transaction::Transaction* transaction,
        const EmbeddingHandle& queryVector, HNSWSearchState& searchState,
        std::vector<NodeWithDistance>& result) const;

    void initLayerSearchState(transaction::Transaction* transaction, HNSWSearchState& searchState,
        bool isUpperLayer) const;
    void oneHopSearch(const EmbeddingHandle& queryVector, graph::Graph::EdgeIterator& nbrItr,
        HNSWSearchState& searchState, min_node_priority_queue_t& candidates,
        max_node_priority_queue_t& results) const;
    void directedTwoHopFilteredSearch(const EmbeddingHandle& queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void blindTwoHopFilteredSearch(const EmbeddingHandle& queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;
    void insertInternal(transaction::Transaction* transaction, common::offset_t offset,
        const EmbeddingHandle& vector, HNSWInsertState& insertState);
    void insertToLayer(transaction::Transaction* transaction, common::offset_t offset,
        common::offset_t entryPoint, const EmbeddingHandle& queryVector,
        HNSWInsertState& insertState, bool isUpperLayer);
    void createRels(transaction::Transaction* transaction, common::offset_t offset,
        const std::vector<NodeWithDistance>& nbrs, bool isUpperLayer, HNSWInsertState& insertState);
    void shrinkForNode(transaction::Transaction* transaction, common::offset_t offset,
        bool isUpperLayer, common::length_t maxDegree, HNSWInsertState& insertState);

    void processSecondHopCandidates(const EmbeddingHandle& queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        const std::vector<common::offset_t>& candidateOffsets) const;
    void processSecondHopCandidates(const EmbeddingHandle& queryVector,
        HNSWSearchState& searchState, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        min_node_priority_queue_t& candidatesQueue) const;

    min_node_priority_queue_t collectFirstHopNbrsDirected(const EmbeddingHandle& queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    common::offset_vec_t collectFirstHopNbrsBlind(const EmbeddingHandle& queryVector,
        graph::Graph::EdgeIterator& nbrItr, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results,
        int64_t& numVisitedNbrs) const;
    // Return false if we've hit Ml limit.
    bool searchOverSecondHopNbrs(const EmbeddingHandle& queryVector, uint64_t ef,
        HNSWSearchState& searchState, common::offset_t cand, int64_t& numVisitedNbrs,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    void initSearchCandidates(const EmbeddingHandle& queryVector, HNSWSearchState& searchState,
        min_node_priority_queue_t& candidates, max_node_priority_queue_t& results) const;

    static SearchType getFilteredSearchType(transaction::Transaction* transaction,
        const HNSWSearchState& searchState);

private:
    static constexpr uint64_t FILTERED_SEARCH_INITIAL_CANDIDATES = 10;
    static constexpr uint64_t INSERTION_BATCH_MERGE_THRESHOLD = 2000;

    storage::MemoryManager* mm;
    storage::NodeTable& nodeTable;
    storage::RelTable* upperRelTable;
    storage::RelTable* lowerRelTable;
};

} // namespace vector_extension
} // namespace kuzu
