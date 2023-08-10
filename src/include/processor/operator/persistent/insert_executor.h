#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// TODO(Guodong): the following class should be moved to storage.
class NodeInsertExecutor {
public:
    NodeInsertExecutor(storage::NodeTable* table,
        std::unique_ptr<evaluator::ExpressionEvaluator> primaryKeyEvaluator,
        std::vector<storage::RelTable*> relTablesToInit, const DataPos& outNodeIDVectorPos)
        : table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{outNodeIDVectorPos} {}
    NodeInsertExecutor(const NodeInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<NodeInsertExecutor> copy() {
        return std::make_unique<NodeInsertExecutor>(*this);
    }

    static std::vector<std::unique_ptr<NodeInsertExecutor>> copy(
        const std::vector<std::unique_ptr<NodeInsertExecutor>>& executors);

private:
    storage::NodeTable* table;
    std::unique_ptr<evaluator::ExpressionEvaluator> primaryKeyEvaluator;
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos outNodeIDVectorPos;

    common::ValueVector* primaryKeyVector = nullptr;
    common::ValueVector* outNodeIDVector = nullptr;
};

class RelInsertExecutor {
public:
    RelInsertExecutor(storage::RelsStatistics& relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::vector<DataPos> lhsVectorPositions,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> evaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, lhsVectorPositions{std::move(lhsVectorPositions)},
          evaluators{std::move(evaluators)} {}
    RelInsertExecutor(const RelInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<RelInsertExecutor> copy() {
        return std::make_unique<RelInsertExecutor>(*this);
    }

    static std::vector<std::unique_ptr<RelInsertExecutor>> copy(
        const std::vector<std::unique_ptr<RelInsertExecutor>>& executors);

private:
    storage::RelsStatistics& relsStatistics;
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    std::vector<DataPos> lhsVectorPositions;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> evaluators;

    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    std::vector<common::ValueVector*> lhsVectors;
    std::vector<common::ValueVector*> rhsVectors;
};

} // namespace processor
} // namespace kuzu
