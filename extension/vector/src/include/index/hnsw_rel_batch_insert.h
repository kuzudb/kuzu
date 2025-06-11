#pragma once

#include "processor/operator/persistent/rel_batch_insert.h"

namespace kuzu {
namespace storage {
class CSRNodeGroup;
struct ChunkedCSRHeader;
} // namespace storage

namespace vector_extension {

struct NodeToHNSWGraphOffsetMap;

class KUZU_API HNSWRelBatchInsert final : public processor::RelBatchInsert {
public:
    HNSWRelBatchInsert(std::string tableName, std::unique_ptr<processor::BatchInsertInfo> info,
        std::shared_ptr<processor::BasePartitionerSharedState> partitionerSharedState,
        std::shared_ptr<processor::BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<processor::RelBatchInsertProgressSharedState> progressSharedState)
        : processor::RelBatchInsert{std::move(tableName), std::move(info),
              std::move(partitionerSharedState), std::move(sharedState), id, std::move(printInfo),
              std::move(progressSharedState)} {}

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<HNSWRelBatchInsert>(tableName, info->copy(), partitionerSharedState,
            sharedState, id, printInfo->copy(), progressSharedState);
    }

    std::unique_ptr<processor::RelBatchInsertExecutionState> initExecutionState(
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
