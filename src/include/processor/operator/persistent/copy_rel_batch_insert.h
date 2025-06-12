#pragma once

#include "processor/operator/persistent/rel_batch_insert.h"

namespace kuzu {
namespace storage {
class CSRNodeGroup;
struct ChunkedCSRHeader;
} // namespace storage

namespace processor {

struct CopyRelBatchInsertExecutionState : RelBatchInsertExecutionState {
    std::unique_ptr<storage::InMemChunkedNodeGroupCollection> partitioningBuffer;
};

class CopyRelBatchInsert final : public RelBatchInsertImpl {
public:
    std::unique_ptr<RelBatchInsertImpl> copy() override {
        return std::make_unique<CopyRelBatchInsert>(*this);
    }

    std::unique_ptr<RelBatchInsertExecutionState> initExecutionState(
        const PartitionerSharedState& partitionerSharedState, const RelBatchInsertInfo& relInfo,
        common::node_group_idx_t nodeGroupIdx) override;

    void populateCSRLengths(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
        const RelBatchInsertInfo& relInfo) override;

    void finalizeStartCSROffsets(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo) override;

    void writeToTable(RelBatchInsertExecutionState& executionState,
        const storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertLocalState& localState,
        BatchInsertSharedState& sharedState, const RelBatchInsertInfo& relInfo) override;

private:
    static void setRowIdxFromCSROffsets(storage::ColumnChunkData& rowIdxChunk,
        storage::ColumnChunkData& csrOffsetChunk);

    static void populateCSRLengthsInternal(const storage::ChunkedCSRHeader& csrHeader,
        common::offset_t numNodes, storage::InMemChunkedNodeGroupCollection& partition,
        common::column_id_t boundNodeOffsetColumn);
};

} // namespace processor
} // namespace kuzu
