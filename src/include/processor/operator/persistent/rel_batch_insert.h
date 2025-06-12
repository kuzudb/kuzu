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

/**
 * Abstract RelBatchInsert class
 * When performing rel batch insert, we typically take some source data and use it to construct the
 * CSR header as well as a chunked node group that actually contains the rel properties
 * Child classes can customize how data is copied from the source into the CSR chunked node group
 * (which is the format in which the rels are actually stored)
 *
 * The following interfaces can be overriden:
 * - initExecutionState(): The execution state contains any extra local state needed during the
 * insertion of a single CSR node group. This reset by calling initExecutionState() before the
 * insertion of each node group so make sure that the lifetime of any stored state doesn't exceed
 * this.
 * - populateCSRLengths(): Populates the length chunk in the CSR header. The offsets are directly
 * calculated from the lengths and thus calculating the offsets doesn't need to be customized
 * - finalizeStartCSROffsets(): The CSR offsets are initially calculated as start offsets. The
 * default behaviour of this function is to convert the start offsets to end offsets. However, if
 * any extra logic is required during this conversion, this function can be overriden.
 * - writeToTable(): Writes property data to the local chunked node group. This function is also
 * responsible for ensuring that the data is written in a way such that the copied data is in
 * agreement with the CSR header
 *
 * Generally, the source data to be copied from should be contained in the partitionerSharedState,
 * which can also be overridden.
 */
class KUZU_API RelBatchInsertImpl {
public:
    virtual ~RelBatchInsertImpl() = default;
    virtual std::unique_ptr<RelBatchInsertImpl> copy() = 0;
    virtual std::unique_ptr<RelBatchInsertExecutionState> initExecutionState(
        const PartitionerSharedState& partitionerSharedState, const RelBatchInsertInfo& relInfo,
        common::node_group_idx_t nodeGroupIdx) = 0;
    virtual void populateCSRLengths(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, common::offset_t numNodes,
        const RelBatchInsertInfo& relInfo) = 0;
    virtual void finalizeStartCSROffsets(RelBatchInsertExecutionState& executionState,
        storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo);
    virtual void writeToTable(RelBatchInsertExecutionState& executionState,
        const storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertLocalState& localState,
        BatchInsertSharedState& sharedState, const RelBatchInsertInfo& relInfo) = 0;
};

class KUZU_API RelBatchInsert : public BatchInsert {
public:
    RelBatchInsert(std::string tableName, std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState,
        std::unique_ptr<RelBatchInsertImpl> impl)
        : BatchInsert{std::move(tableName), std::move(info), std::move(sharedState), id,
              std::move(printInfo)},
          partitionerSharedState{std::move(partitionerSharedState)},
          progressSharedState{std::move(progressSharedState)}, impl(std::move(impl)) {}

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<RelBatchInsert>(tableName, info->copy(), partitionerSharedState,
            sharedState, id, printInfo->copy(), progressSharedState, impl->copy());
    }

    bool isSource() const override { return true; }

    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalizeInternal(ExecutionContext* context) override;

    void updateProgress(const ExecutionContext* context) const;

private:
    void appendNodeGroup(const catalog::RelGroupCatalogEntry& relGroupEntry,
        storage::MemoryManager& mm, transaction::Transaction* transaction,
        storage::CSRNodeGroup& nodeGroup, const RelBatchInsertInfo& relInfo,
        const RelBatchInsertLocalState& localState);

    void populateCSRHeader(const catalog::RelGroupCatalogEntry& relGroupEntry,
        RelBatchInsertExecutionState& executionState, common::offset_t startNodeOffset,
        const RelBatchInsertInfo& relInfo, const RelBatchInsertLocalState& localState,
        common::offset_t numNodes, bool leaveGaps);

    static void checkRelMultiplicityConstraint(const catalog::RelGroupCatalogEntry& relGroupEntry,
        const storage::ChunkedCSRHeader& csrHeader, common::offset_t startNodeOffset,
        const RelBatchInsertInfo& relInfo);

protected:
    std::shared_ptr<PartitionerSharedState> partitionerSharedState;
    std::shared_ptr<RelBatchInsertProgressSharedState> progressSharedState;
    std::unique_ptr<RelBatchInsertImpl> impl;
};

} // namespace processor
} // namespace kuzu
