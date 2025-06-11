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

class CopyRelBatchInsert final : public RelBatchInsert {
public:
    CopyRelBatchInsert(std::string tableName, std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState)
        : RelBatchInsert{std::move(tableName), std::move(info), std::move(partitionerSharedState),
              std::move(sharedState), id, std::move(printInfo), std::move(progressSharedState)} {}

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<CopyRelBatchInsert>(tableName, info->copy(), partitionerSharedState,
            sharedState, id, printInfo->copy(), progressSharedState);
    }

    std::unique_ptr<RelBatchInsertExecutionState> initExecutionState(
        const RelBatchInsertInfo& relInfo, common::node_group_idx_t nodeGroupIdx) override;

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
