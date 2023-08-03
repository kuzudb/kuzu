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
        std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator,
        std::vector<storage::RelTable*> relTablesToInit, const DataPos& outNodeIDVectorPos)
        : table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{outNodeIDVectorPos} {}
    NodeInsertExecutor(const NodeInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<NodeInsertExecutor> copy() {
        return std::make_unique<NodeInsertExecutor>(*this);
    }

private:
    storage::NodeTable* table;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator;
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos outNodeIDVectorPos;

    common::ValueVector* primaryKeyVector = nullptr;
    common::ValueVector* outNodeIDVector = nullptr;
};

class RelInsertExecutor {
public:
    RelInsertExecutor(storage::RelsStatistics& relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, evaluators{std::move(evaluators)} {}
    RelInsertExecutor(const RelInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<RelInsertExecutor> copy() {
        return std::make_unique<RelInsertExecutor>(*this);
    }

private:
    storage::RelsStatistics& relsStatistics;
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators;

    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    std::vector<common::ValueVector*> propertyVectors;
};

} // namespace processor
} // namespace kuzu
