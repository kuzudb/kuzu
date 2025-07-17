#pragma once

#include "index/hnsw_index.h"
#include "processor/operator/persistent/rel_batch_insert.h"

namespace kuzu {
namespace storage {
class CSRNodeGroup;
struct ChunkedCSRHeader;
} // namespace storage

namespace vector_extension {

struct HNSWLayerPartitionerSharedState : processor::PartitionerSharedState {
    std::unique_ptr<InMemHNSWGraph> graph;
    std::unique_ptr<NodeToHNSWGraphOffsetMap> graphSelectionMap;

    std::atomic<common::partition_idx_t> nextPartitionIdx;

    HNSWLayerPartitionerSharedState() = default;

    void setGraph(std::unique_ptr<InMemHNSWGraph> newGraph,
        std::unique_ptr<NodeToHNSWGraphOffsetMap> selectionMap);

    void resetState(common::idx_t partitioningIdx) override;
};

struct HNSWIndexPartitionerSharedState {
    std::shared_ptr<HNSWLayerPartitionerSharedState> lowerPartitionerSharedState;
    std::shared_ptr<HNSWLayerPartitionerSharedState> upperPartitionerSharedState;

    explicit HNSWIndexPartitionerSharedState()
        : lowerPartitionerSharedState{std::make_shared<HNSWLayerPartitionerSharedState>()},
          upperPartitionerSharedState{std::make_shared<HNSWLayerPartitionerSharedState>()} {}

    void setTables(storage::NodeTable* nodeTable, storage::RelTable* upperRelTable,
        storage::RelTable* lowerRelTable);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void initialize(const common::logical_type_vec_t& columnTypes,
        const main::ClientContext* clientContext) {
        lowerPartitionerSharedState->initialize(columnTypes, 1 /*numPartitioners*/, clientContext);
        upperPartitionerSharedState->initialize(columnTypes, 1 /*numPartitioners*/, clientContext);
    }
};

class HNSWRelBatchInsert final : public processor::RelBatchInsertImpl {
public:
    std::unique_ptr<processor::RelBatchInsertImpl> copy() override;

    std::unique_ptr<processor::RelBatchInsertExecutionState> initExecutionState(
        const processor::PartitionerSharedState& partitionerSharedState,
        const processor::RelBatchInsertInfo& relInfo,
        common::node_group_idx_t nodeGroupIdx) override;

    void populateCSRLengths(processor::RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
        const processor::RelBatchInsertInfo& relInfo) override;

    void writeToTable(processor::RelBatchInsertExecutionState& executionState,
        const storage::ChunkedCSRHeader& csrHeader,
        const processor::RelBatchInsertLocalState& localState,
        processor::BatchInsertSharedState& sharedState,
        const processor::RelBatchInsertInfo& relInfo) override;
};

} // namespace vector_extension
} // namespace kuzu
