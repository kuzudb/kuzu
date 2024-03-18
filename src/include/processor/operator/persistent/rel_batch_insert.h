#pragma once

#include "common/enums/rel_direction.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/batch_insert.h"
#include "storage/store/column_chunk.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace processor {

struct RelBatchInsertInfo final : public BatchInsertInfo {
    common::RelDataDirection direction;
    uint64_t partitioningIdx;
    common::vector_idx_t offsetVectorIdx;
    std::vector<common::LogicalType> columnTypes;

    RelBatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        common::RelDataDirection direction, uint64_t partitioningIdx,
        common::vector_idx_t offsetVectorIdx, std::vector<common::LogicalType> columnTypes)
        : BatchInsertInfo{tableEntry, compressionEnabled}, direction{direction},
          partitioningIdx{partitioningIdx}, offsetVectorIdx{offsetVectorIdx}, columnTypes{std::move(
                                                                                  columnTypes)} {}
    RelBatchInsertInfo(const RelBatchInsertInfo& other)
        : BatchInsertInfo{other.tableEntry, other.compressionEnabled}, direction{other.direction},
          partitioningIdx{other.partitioningIdx}, offsetVectorIdx{other.offsetVectorIdx},
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
        const std::string& paramsString)
        : BatchInsert{std::move(info), std::move(sharedState), std::move(resultSetDescriptor), id,
              paramsString},
          partitionerSharedState{std::move(partitionerSharedState)} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;
    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RelBatchInsert>(info->copy(), partitionerSharedState, sharedState,
            resultSetDescriptor->copy(), id, paramsString);
    }

private:
    void prepareCSRNodeGroup(PartitioningBuffer::Partition& partition,
        common::offset_t startNodeOffset, common::vector_idx_t offsetVectorIdx,
        common::offset_t numNodes);

    static common::length_t getGapSize(common::length_t length);
    static std::vector<common::offset_t> populateStartCSROffsetsAndLengths(
        storage::CSRHeaderChunks& csrHeader, common::offset_t numNodes,
        PartitioningBuffer::Partition& partition, common::vector_idx_t offsetVectorIdx);
    static void populateEndCSROffsets(
        storage::CSRHeaderChunks& csrHeader, std::vector<common::offset_t>& gaps);
    static void setOffsetToWithinNodeGroup(
        storage::ColumnChunk& chunk, common::offset_t startOffset);
    static void setOffsetFromCSROffsets(
        storage::ColumnChunk* nodeOffsetChunk, storage::ColumnChunk* csrOffsetChunk);

    // We only check rel multiplcity constraint (MANY_ONE, ONE_ONE) for now.
    std::optional<common::offset_t> checkRelMultiplicityConstraint(
        const storage::CSRHeaderChunks& csrHeader);

private:
    std::shared_ptr<PartitionerSharedState> partitionerSharedState;
};

} // namespace processor
} // namespace kuzu
