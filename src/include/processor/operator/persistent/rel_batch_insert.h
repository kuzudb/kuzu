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
    common::table_id_t fromTableID, toTableID;
    uint64_t partitioningIdx;
    common::column_id_t boundNodeOffsetColumnID;

    RelBatchInsertInfo(std::string tableName, bool compressionEnabled,
        common::RelDataDirection direction, common::table_id_t fromTableID,
        common::table_id_t toTableID, uint64_t partitioningIdx, common::column_id_t offsetColumnID,
        std::vector<common::column_id_t> columnIDs, std::vector<common::LogicalType> columnTypes,
        common::column_id_t numWarningDataColumns)
        : BatchInsertInfo{std::move(tableName), compressionEnabled, std::move(columnIDs),
              std::move(columnTypes), numWarningDataColumns},
          direction{direction}, fromTableID{fromTableID}, toTableID{toTableID},
          partitioningIdx{partitioningIdx}, boundNodeOffsetColumnID{offsetColumnID} {}
    RelBatchInsertInfo(const RelBatchInsertInfo& other)
        : BatchInsertInfo{other}, direction{other.direction}, fromTableID{other.fromTableID},
          toTableID{other.toTableID}, partitioningIdx{other.partitioningIdx},
          boundNodeOffsetColumnID{other.boundNodeOffsetColumnID} {}

    std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<RelBatchInsertInfo>(*this);
    }
};

struct KUZU_API RelBatchInsertLocalState final : BatchInsertLocalState {
    common::partition_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    std::unique_ptr<common::DataChunk> dummyAllNullDataChunk;
};

struct KUZU_API RelBatchInsertExecutionState {
    virtual ~RelBatchInsertExecutionState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

// TODO(Royi) add comment describing interfaces to implement
class KUZU_API RelBatchInsert : public BatchInsert {
public:
    RelBatchInsert(std::string tableName, std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BasePartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState)
        : BatchInsert{std::move(tableName), std::move(info), std::move(sharedState), id,
              std::move(printInfo)},
          partitionerSharedState{std::move(partitionerSharedState)},
          progressSharedState{std::move(progressSharedState)} {}

    bool isSource() const override { return true; }

    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalizeInternal(ExecutionContext* context) override;

    void updateProgress(const ExecutionContext* context) const;

    virtual std::unique_ptr<RelBatchInsertExecutionState> initExecutionState(
        const RelBatchInsertInfo& relInfo, common::node_group_idx_t nodeGroupIdx) = 0;
    virtual void populateCSRLengths(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
        const RelBatchInsertInfo& relInfo) = 0;
    virtual void finalizeStartCSROffsets(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo);
    virtual void writeToTable(RelBatchInsertExecutionState& executionState,
        const storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertLocalState& localState,
        BatchInsertSharedState& sharedState, const RelBatchInsertInfo& relInfo) = 0;

private:
    void appendNodeGroup(const catalog::RelGroupCatalogEntry& relGroupEntry,
        storage::MemoryManager& mm, transaction::Transaction* transaction,
        storage::CSRNodeGroup& nodeGroup, const RelBatchInsertInfo& relInfo,
        const RelBatchInsertLocalState& localState, BatchInsertSharedState& sharedState,
        const BasePartitionerSharedState& partitionerSharedState);

    void populateCSRHeaderAndRowIdx(const catalog::RelGroupCatalogEntry& relGroupEntry,
        RelBatchInsertExecutionState& executionState, common::offset_t startNodeOffset,
        const RelBatchInsertInfo& relInfo, const RelBatchInsertLocalState& localState,
        common::offset_t numNodes, bool leaveGaps);

    static void checkRelMultiplicityConstraint(const catalog::RelGroupCatalogEntry& relGroupEntry,
        const storage::ChunkedCSRHeader& csrHeader, common::offset_t startNodeOffset,
        const RelBatchInsertInfo& relInfo);

protected:
    std::shared_ptr<BasePartitionerSharedState> partitionerSharedState;
    std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState;
};

} // namespace processor
} // namespace kuzu
