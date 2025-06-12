#pragma once

#include "processor/operator/persistent/rel_batch_insert.h"

namespace kuzu {
namespace storage {
class CSRNodeGroup;
struct ChunkedCSRHeader;
} // namespace storage

namespace vector_extension {

struct NodeToHNSWGraphOffsetMap;

class KUZU_API HNSWRelBatchInsert final : public processor::RelBatchInsertImpl {
public:
    HNSWRelBatchInsert();
    HNSWRelBatchInsert(const HNSWRelBatchInsert&);
    HNSWRelBatchInsert& operator=(const HNSWRelBatchInsert&);
    ~HNSWRelBatchInsert() override;

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
