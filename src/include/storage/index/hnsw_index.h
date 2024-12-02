#pragma once

#include "common/random_engine.h"
#include "storage/index/hnsw_config.h"
#include "storage/index/hnsw_graph.h"

namespace kuzu {
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

// TODO(Guodong): Is this the right place for this struct?
struct HNSWIndexPartitionerSharedState {
    std::shared_ptr<processor::PartitionerSharedState> lowerPartitionerSharedState;
    std::shared_ptr<processor::PartitionerSharedState> upperPartitionerSharedState;

    explicit HNSWIndexPartitionerSharedState(MemoryManager& mm)
        : lowerPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)},
          upperPartitionerSharedState{std::make_shared<processor::PartitionerSharedState>(mm)} {}

    void setTables(NodeTable* nodeTable, RelTable* relTable);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void initialize(const common::logical_type_vec_t& columnTypes,
        main::ClientContext* clientContext) {
        lowerPartitionerSharedState->initialize(columnTypes, clientContext);
        upperPartitionerSharedState->initialize(columnTypes, clientContext);
    }
};

class HNSWIndex {
public:
    explicit HNSWIndex(HNSWIndexConfig config) : config{std::move(config)} {}
    virtual ~HNSWIndex() = default;

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void initialize(main::ClientContext* context, NodeTable& table, common::column_id_t columnID) {
        embeddings->initialize(context, table, columnID);
    }

    common::offset_t getUpperEntryPoint() const { return upperGraph->getEntryPoint(); }
    common::offset_t getLowerEntryPoint() const { return lowerGraph->getEntryPoint(); }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void setUpperEntryPoint(common::offset_t offset) { upperGraph->setEntryPoint(offset); }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void setLowerEntryPoint(common::offset_t offset) { lowerGraph->setEntryPoint(offset); }

protected:
    std::vector<NodeWithDistance> searchLowerLayer(const float* queryVector,
        common::offset_t entryNode, common::length_t k, uint64_t configuredEfs,
        transaction::Transaction* transaction) const;
    common::offset_t searchUpperLayer(const float* queryVector,
        transaction::Transaction* transaction) const;
    static std::vector<NodeWithDistance> gatherTopK(max_node_priority_queue_t& result,
        common::length_t k);

protected:
    std::unique_ptr<EmbeddingColumn> embeddings;
    std::unique_ptr<HNSWGraph> lowerGraph;
    std::unique_ptr<HNSWGraph> upperGraph;
    HNSWIndexConfig config;
};

class InMemHNSWIndex final : public HNSWIndex {
public:
    InMemHNSWIndex(main::ClientContext* context, NodeTable& table, common::column_id_t columnID,
        HNSWIndexConfig config);

    // Note that the input is only `offset`, as we assume embeddings are already cached in memory.
    void insert(common::offset_t offset, transaction::Transaction* transaction);
    void shrink(transaction::Transaction* transaction);
    void finalize(MemoryManager& mm, const HNSWIndexPartitionerSharedState& partitionerSharedState);

private:
    // Return the entry point of the lower layer graph that is searched from the upper layer graph.
    common::offset_t insertToLowerLayer(common::offset_t offset,
        transaction::Transaction* transaction);
    void insertToUpperLayer(common::offset_t offset, common::offset_t entryNode,
        transaction::Transaction* transaction);

private:
    static constexpr int64_t INSERT_TO_UPPER_LAYER_PROB_THRESHOLD = 95;
    static constexpr int64_t INSERT_TO_UPPER_LAYER_RAND_UPPER_BOUND = 100;

    common::RandomEngine randomEngine;
};

class OnDiskHNSWIndex final : public HNSWIndex {
public:
    OnDiskHNSWIndex(main::ClientContext* context, NodeTable& nodeTable,
        common::column_id_t columnID, RelTable& upperRelTable, RelTable& lowerRelTable,
        HNSWIndexConfig config);

    std::vector<NodeWithDistance> search(const std::vector<float>& queryVector, common::length_t k,
        const QueryHNSWConfig& config, transaction::Transaction* transaction) const;
};

} // namespace storage
} // namespace kuzu
