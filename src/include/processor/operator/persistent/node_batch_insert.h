#pragma once

#include "common/enums/column_evaluate_type.h"
#include "common/types/types.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/persistent/index_builder.h"
#include "processor/operator/table_function_call.h"
#include "storage/store/chunked_node_group.h"

namespace kuzu {
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
    std::vector<common::LogicalType> columnTypes;
    evaluator::evaluator_vector_t columnEvaluators;
    std::vector<common::ColumnEvaluateType> evaluateTypes;

    NodeBatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        std::vector<common::LogicalType> columnTypes,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators,
        std::vector<common::ColumnEvaluateType> evaluateTypes, bool ignoreErrors)
        : BatchInsertInfo{tableEntry, compressionEnabled, ignoreErrors},
          columnTypes{std::move(columnTypes)}, columnEvaluators{std::move(columnEvaluators)},
          evaluateTypes{std::move(evaluateTypes)} {}

    NodeBatchInsertInfo(const NodeBatchInsertInfo& other)
        : BatchInsertInfo{other.tableEntry, other.compressionEnabled, other.ignoreErrors},
          columnTypes{common::LogicalType::copy(other.columnTypes)},
          columnEvaluators{cloneVector(other.columnEvaluators)},
          evaluateTypes{other.evaluateTypes} {}

    std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<NodeBatchInsertInfo>(*this);
    }
};

struct NodeBatchInsertSharedState final : BatchInsertSharedState {
    // Primary key info
    common::column_id_t pkColumnID;
    common::LogicalType pkType;
    std::optional<IndexBuilder> globalIndexBuilder = std::nullopt;

    TableFunctionCallSharedState* readerSharedState;
    HashAggregateSharedState* distinctSharedState;

    // The sharedNodeGroup is to accumulate left data within local node groups in NodeBatchInsert
    // ops.
    std::unique_ptr<storage::ChunkedNodeGroup> sharedNodeGroup;

    NodeBatchInsertSharedState(storage::Table* table, common::column_id_t pkColumnID,
        common::LogicalType pkType, std::shared_ptr<FactorizedTable> fTable, storage::WAL* wal)
        : BatchInsertSharedState{table, std::move(fTable), wal}, pkColumnID{pkColumnID},
          pkType{std::move(pkType)}, readerSharedState{nullptr}, distinctSharedState{nullptr},
          sharedNodeGroup{nullptr} {}

    void initPKIndex(const ExecutionContext* context);
};

struct NodeBatchInsertLocalState final : BatchInsertLocalState {
    std::optional<NodeBatchInsertErrorHandler> errorHandler;

    std::optional<IndexBuilder> localIndexBuilder;

    std::shared_ptr<common::DataChunkState> columnState;
    std::vector<common::ValueVector*> columnVectors;
};

class NodeBatchInsert final : public BatchInsert {
public:
    NodeBatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BatchInsert{std::move(info), std::move(sharedState), std::move(resultSetDescriptor), id,
              std::move(printInfo)} {
        children.push_back(std::move(child));
    }

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<NodeBatchInsert>(info->copy(), sharedState,
            resultSetDescriptor->copy(), children[0]->clone(), id, printInfo->copy());
    }

    // The node group will be reset so that the only values remaining are the ones which were not
    // written
    void writeAndResetNodeGroup(transaction::Transaction* transaction,
        std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        std::optional<IndexBuilder>& indexBuilder) const;

private:
    void appendIncompleteNodeGroup(transaction::Transaction* transaction,
        std::unique_ptr<storage::ChunkedNodeGroup> localNodeGroup,
        std::optional<IndexBuilder>& indexBuilder) const;
    void clearToIndex(std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        common::offset_t startIndexInGroup) const;

    void copyToNodeGroup(transaction::Transaction* transaction) const;
};

} // namespace processor
} // namespace kuzu
