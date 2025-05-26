#pragma once

#include "common/enums/rel_direction.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/batch_insert.h"

namespace kuzu {
namespace storage {
class CSRNodeGroup;
struct ChunkedCSRHeader;
} // namespace storage

namespace processor {

struct KUZU_API RelBatchInsertPrintInfo final : OPPrintInfo {
    std::string tableName;

    explicit RelBatchInsertPrintInfo(std::string tableName) : tableName(std::move(tableName)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<RelBatchInsertPrintInfo>(new RelBatchInsertPrintInfo(*this));
    }

private:
    RelBatchInsertPrintInfo(const RelBatchInsertPrintInfo& other)
        : OPPrintInfo(other), tableName(other.tableName) {}
};

struct KUZU_API RelBatchInsertProgressSharedState {
    std::atomic<uint64_t> partitionsDone;
    uint64_t partitionsTotal;

    RelBatchInsertProgressSharedState() : partitionsDone{0}, partitionsTotal{0} {};
};

struct KUZU_API RelBatchInsertInfo final : BatchInsertInfo {
    common::RelDataDirection direction;
    uint64_t partitioningIdx = UINT64_MAX;
    common::column_id_t boundNodeOffsetColumnID = common::INVALID_COLUMN_ID;

    RelBatchInsertInfo(std::string tableName, std::vector<common::LogicalType> warningColumnTypes,
        common::RelDataDirection direction)
        : BatchInsertInfo{tableName, std::move(warningColumnTypes)}, direction{direction} {}
    RelBatchInsertInfo(const RelBatchInsertInfo& other)
        : BatchInsertInfo{other}, direction{other.direction},
          partitioningIdx{other.partitioningIdx},
          boundNodeOffsetColumnID{other.boundNodeOffsetColumnID} {}

    std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<RelBatchInsertInfo>(*this);
    }
};

struct RelBatchInsertLocalState final : BatchInsertLocalState {
    common::partition_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    std::unique_ptr<common::DataChunk> dummyAllNullDataChunk;
};

class KUZU_API RelBatchInsert final : public BatchInsert {
public:
    RelBatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState)
        : BatchInsert{std::move(info), std::move(sharedState), id, std::move(printInfo)},
          partitionerSharedState{std::move(partitionerSharedState)},
          progressSharedState{std::move(progressSharedState)} {}

    bool isSource() const override { return true; }

    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<RelBatchInsert>(info->copy(), partitionerSharedState, sharedState,
            id, printInfo->copy(), progressSharedState);
    }

    void updateProgress(const ExecutionContext* context) const;

private:
    static void appendNodeGroup(storage::MemoryManager& mm, transaction::Transaction* transaction,
        storage::CSRNodeGroup& nodeGroup, const RelBatchInsertInfo& relInfo,
        const RelBatchInsertLocalState& localState, BatchInsertSharedState& sharedState,
        const PartitionerSharedState& partitionerSharedState);

    static void populateCSRHeaderAndRowIdx(storage::InMemChunkedNodeGroupCollection& partition,
        common::offset_t startNodeOffset, const RelBatchInsertInfo& relInfo,
        const RelBatchInsertLocalState& localState, common::offset_t numNodes, bool leaveGaps);

    static void populateCSRLengths(const storage::ChunkedCSRHeader& csrHeader,
        common::offset_t numNodes, storage::InMemChunkedNodeGroupCollection& partition,
        common::column_id_t boundNodeOffsetColumn);

    static void setOffsetToWithinNodeGroup(storage::ColumnChunkData& chunk,
        common::offset_t startOffset);
    static void setRowIdxFromCSROffsets(storage::ColumnChunkData& rowIdxChunk,
        storage::ColumnChunkData& csrOffsetChunk);

    static void checkRelMultiplicityConstraint(const storage::ChunkedCSRHeader& csrHeader,
        common::offset_t startNodeOffset, const RelBatchInsertInfo& relInfo);

private:
    std::shared_ptr<PartitionerSharedState> partitionerSharedState;
    std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState;
};

} // namespace processor
} // namespace kuzu
