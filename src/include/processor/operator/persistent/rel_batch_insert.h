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

struct RelBatchInsertPrintInfo final : OPPrintInfo {
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

struct RelBatchInsertInfo final : BatchInsertInfo {
    common::RelDataDirection direction;
    uint64_t partitioningIdx;
    common::column_id_t boundNodeOffsetColumnID;
    std::vector<common::LogicalType> columnTypes;

    RelBatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        common::RelDataDirection direction, uint64_t partitioningIdx,
        common::column_id_t offsetColumnID, std::vector<common::LogicalType> columnTypes)
        : BatchInsertInfo{tableEntry, compressionEnabled, false}, direction{direction},
          partitioningIdx{partitioningIdx}, boundNodeOffsetColumnID{offsetColumnID},
          columnTypes{std::move(columnTypes)} {}
    RelBatchInsertInfo(const RelBatchInsertInfo& other)
        : BatchInsertInfo{other.tableEntry, other.compressionEnabled, other.ignoreErrors},
          direction{other.direction}, partitioningIdx{other.partitioningIdx},
          boundNodeOffsetColumnID{other.boundNodeOffsetColumnID},
          columnTypes{common::LogicalType::copy(other.columnTypes)} {}

    std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<RelBatchInsertInfo>(*this);
    }
};

struct RelBatchInsertLocalState final : BatchInsertLocalState {
    common::partition_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    std::unique_ptr<common::DataChunk> dummyAllNullDataChunk;
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

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RelBatchInsert>(info->copy(), partitionerSharedState, sharedState,
            resultSetDescriptor->copy(), id, printInfo->copy());
    }

private:
    static void appendNodeGroup(transaction::Transaction* transaction,
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
};

} // namespace processor
} // namespace kuzu
