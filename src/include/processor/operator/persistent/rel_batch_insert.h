#pragma once

#include "common/enums/rel_direction.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/batch_insert.h"
#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace processor {

struct RelBatchInsertInfo final : public BatchInsertInfo {
    common::RelDataDirection direction;
    uint64_t partitioningIdx;
    common::column_id_t offsetColumnID;
    std::vector<common::LogicalType> columnTypes;

    RelBatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        common::RelDataDirection direction, uint64_t partitioningIdx,
        common::column_id_t offsetColumnID, std::vector<common::LogicalType> columnTypes)
        : BatchInsertInfo{tableEntry, compressionEnabled}, direction{direction},
          partitioningIdx{partitioningIdx}, offsetColumnID{offsetColumnID},
          columnTypes{std::move(columnTypes)} {}
    RelBatchInsertInfo(const RelBatchInsertInfo& other)
        : BatchInsertInfo{other.tableEntry, other.compressionEnabled}, direction{other.direction},
          partitioningIdx{other.partitioningIdx}, offsetColumnID{other.offsetColumnID},
          columnTypes{common::LogicalType::copy(other.columnTypes)} {}

    inline std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<RelBatchInsertInfo>(*this);
    }
};

struct RelBatchInsertLocalState final : public BatchInsertLocalState {
    common::partition_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
};

class RelBatchInsert final : public BatchInsert {
public:
    RelBatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BatchInsert{std::move(info), std::move(sharedState), std::move(resultSetDescriptor), id,
              std::move(printInfo)},
          partitionerSharedState{std::move(partitionerSharedState)} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;
    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RelBatchInsert>(info->copy(), partitionerSharedState, sharedState,
            resultSetDescriptor->copy(), id, printInfo->copy());
    }

private:
    static void appendNewNodeGroup(transaction::Transaction* transaction,
        const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState,
        BatchInsertSharedState& sharedState, const PartitionerSharedState& partitionerSharedState);
    static void mergeNodeGroup(transaction::Transaction* transaction,
        const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState,
        BatchInsertSharedState& sharedState, const PartitionerSharedState& partitionerSharedState);

    static void prepareCSRNodeGroup(const storage::ChunkedNodeGroupCollection& partition,
        common::offset_t startNodeOffset, const RelBatchInsertInfo& relInfo,
        RelBatchInsertLocalState& localState, common::offset_t numNodes);

    static common::length_t getGapSize(common::length_t length);
    static std::vector<common::offset_t> populateStartCSROffsetsAndLengths(
        storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
        const storage::ChunkedNodeGroupCollection& partition, common::column_id_t offsetColumnID);
    static void populateEndCSROffsets(storage::ChunkedCSRHeader& csrHeader,
        std::vector<common::offset_t>& gaps);
    static void setOffsetToWithinNodeGroup(storage::ColumnChunkData& chunk,
        common::offset_t startOffset);
    static void setOffsetFromCSROffsets(storage::ColumnChunkData& nodeOffsetChunk,
        storage::ColumnChunkData& csrOffsetChunk);

    static std::optional<common::offset_t> checkRelMultiplicityConstraint(
        const storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo);

private:
    std::shared_ptr<PartitionerSharedState> partitionerSharedState;
};

} // namespace processor
} // namespace kuzu
