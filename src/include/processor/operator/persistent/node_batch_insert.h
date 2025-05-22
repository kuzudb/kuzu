#pragma once

#include "common/enums/column_evaluate_type.h"
#include "common/types/types.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/persistent/index_builder.h"
#include "storage/stats/table_stats.h"
#include "storage/table/chunked_node_group.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace transaction {
class Transaction;
} // namespace transaction

namespace processor {
struct ExecutionContext;

struct NodeBatchInsertPrintInfo final : OPPrintInfo {
    std::string tableName;

    explicit NodeBatchInsertPrintInfo(std::string tableName) : tableName(std::move(tableName)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<NodeBatchInsertPrintInfo>(new NodeBatchInsertPrintInfo(*this));
    }

private:
    NodeBatchInsertPrintInfo(const NodeBatchInsertPrintInfo& other)
        : OPPrintInfo(other), tableName(other.tableName) {}
};

struct NodeBatchInsertInfo final : BatchInsertInfo {
    evaluator::evaluator_vector_t columnEvaluators;
    std::vector<common::ColumnEvaluateType> evaluateTypes;

    NodeBatchInsertInfo(bool compressionEnabled, std::vector<common::LogicalType> columnTypes,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators,
        std::vector<common::ColumnEvaluateType> evaluateTypes,
        common::column_id_t numWarningDataColumns)
        : BatchInsertInfo{nullptr, compressionEnabled, {}, std::move(columnTypes),
              numWarningDataColumns},
          columnEvaluators{std::move(columnEvaluators)}, evaluateTypes{std::move(evaluateTypes)} {}

    NodeBatchInsertInfo(const NodeBatchInsertInfo& other)
        : BatchInsertInfo{other}, columnEvaluators{copyVector(other.columnEvaluators)},
          evaluateTypes{other.evaluateTypes} {}

    std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<NodeBatchInsertInfo>(*this);
    }
};

struct NodeBatchInsertSharedState final : BatchInsertSharedState {
    // Primary key info
    common::column_id_t pkColumnID;
    common::LogicalType pkType;
    std::optional<IndexBuilder> globalIndexBuilder;

    function::TableFuncSharedState* tableFuncSharedState;

    std::vector<common::column_id_t> mainDataColumns;

    // The sharedNodeGroup is to accumulate left data within local node groups in NodeBatchInsert
    // ops.
    std::unique_ptr<storage::ChunkedNodeGroup> sharedNodeGroup;

    NodeBatchInsertSharedState(std::shared_ptr<FactorizedTable> fTable, storage::WAL* wal,
        storage::MemoryManager* mm)
        : BatchInsertSharedState{nullptr, std::move(fTable), wal, mm}, pkColumnID{0},
          globalIndexBuilder(std::nullopt), tableFuncSharedState{nullptr},
          sharedNodeGroup{nullptr} {}

    void initPKIndex(const ExecutionContext* context);
};

struct NodeBatchInsertLocalState final : BatchInsertLocalState {
    std::optional<NodeBatchInsertErrorHandler> errorHandler;

    std::optional<IndexBuilder> localIndexBuilder;

    std::shared_ptr<common::DataChunkState> columnState;
    std::vector<common::ValueVector*> columnVectors;

    storage::TableStats stats;

    explicit NodeBatchInsertLocalState(std::span<common::LogicalType> outputDataTypes)
        : stats{outputDataTypes} {}
};

class NodeBatchInsert final : public BatchInsert {
public:
    NodeBatchInsert(std::string tableName, std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BatchInsert{std::move(tableName), std::move(info), std::move(sharedState), id,
              std::move(printInfo)} {
        children.push_back(std::move(child));
    }

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;
    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<NodeBatchInsert>(tableName, info->copy(), sharedState,
            children[0]->copy(), id, printInfo->copy());
    }

    // The node group will be reset so that the only values remaining are the ones which were
    // not written
    void writeAndResetNodeGroup(transaction::Transaction* transaction,
        std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        std::optional<IndexBuilder>& indexBuilder, storage::MemoryManager* mm) const;

private:
    void evaluateExpressions(uint64_t numTuples) const;
    void appendIncompleteNodeGroup(transaction::Transaction* transaction,
        std::unique_ptr<storage::ChunkedNodeGroup> localNodeGroup,
        std::optional<IndexBuilder>& indexBuilder, storage::MemoryManager* mm) const;
    void clearToIndex(storage::MemoryManager* mm,
        std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        common::offset_t startIndexInGroup) const;

    void copyToNodeGroup(transaction::Transaction* transaction, storage::MemoryManager* mm) const;

    NodeBatchInsertErrorHandler createErrorHandler(ExecutionContext* context) const;

    void writeAndResetNodeGroup(transaction::Transaction* transaction,
        std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        std::optional<IndexBuilder>& indexBuilder, storage::MemoryManager* mm,
        NodeBatchInsertErrorHandler& errorHandler) const;
};

} // namespace processor
} // namespace kuzu
