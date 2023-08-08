#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// TODO(Guodong): the following class should be moved to storage.
class NodeInsertExecutor {
public:
    NodeInsertExecutor(storage::NodeTable* table, std::vector<storage::RelTable*> relTablesToInit,
        const DataPos& nodeIDVectorPos, std::vector<DataPos> propertyLhsPositions,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators,
        std::unordered_map<common::property_id_t, common::vector_idx_t> propertyIDToVectorIdx)
        : table{table}, relTablesToInit{std::move(relTablesToInit)},
          nodeIDVectorPos{nodeIDVectorPos}, propertyLhsPositions{std::move(propertyLhsPositions)},
          propertyRhsEvaluators{std::move(propertyRhsEvaluators)}, propertyIDToVectorIdx{std::move(
                                                                       propertyIDToVectorIdx)} {}
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
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos nodeIDVectorPos;
    std::vector<DataPos> propertyLhsPositions;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators;
    // TODO(Guodong): remove this.
    std::unordered_map<common::property_id_t, common::vector_idx_t> propertyIDToVectorIdx;

    common::ValueVector* nodeIDVector;
    std::vector<common::ValueVector*> propertyLhsVectors;
    std::vector<common::ValueVector*> propertyRhsVectors;
};

class RelInsertExecutor {
public:
    RelInsertExecutor(storage::RelsStatistics& relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::vector<DataPos> propertyLhsPositions,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, propertyLhsPositions{std::move(propertyLhsPositions)},
          propertyRhsEvaluators{std::move(propertyRhsEvaluators)} {}
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
    std::vector<DataPos> propertyLhsPositions;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators;

    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    std::vector<common::ValueVector*> propertyLhsVectors;
    std::vector<common::ValueVector*> propertyRhsVectors;
};

} // namespace processor
} // namespace kuzu
