#pragma once

#include "common/enums/conflict_action.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class NodeInsertExecutor {
public:
    NodeInsertExecutor(storage::NodeTable* table, const DataPos& nodeIDVectorPos,
        std::vector<DataPos> columnVectorsPos,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnDataEvaluators,
        common::ConflictAction conflictAction)
        : table{table}, nodeIDVectorPos{nodeIDVectorPos},
          columnVectorsPos{std::move(columnVectorsPos)},
          columnDataEvaluators{std::move(columnDataEvaluators)}, conflictAction{conflictAction},
          nodeIDVector{nullptr} {}
    EXPLICIT_COPY_DEFAULT_MOVE(NodeInsertExecutor);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    // For MERGE, we might need to skip the insert for duplicate input. But still, we need to write
    // the output vector for later usage.
    void skipInsert();

private:
    NodeInsertExecutor(const NodeInsertExecutor& other);

    bool checkConflict(transaction::Transaction* transaction);

    void writeResult();

private:
    // Node table to insert.
    storage::NodeTable* table;

    DataPos nodeIDVectorPos;
    // Column vector pos is invalid if it doesn't need to be projected.
    std::vector<DataPos> columnVectorsPos;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnDataEvaluators;

    common::ConflictAction conflictAction;

    common::ValueVector* nodeIDVector;
    std::vector<common::ValueVector*> columnVectors;
    std::vector<common::ValueVector*> columnDataVectors;
};

class RelInsertExecutor {
public:
    RelInsertExecutor(storage::RelsStoreStats* relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos, std::vector<DataPos> columnVectorsPos,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnDataEvaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, columnVectorsPos{std::move(columnVectorsPos)},
          columnDataEvaluators{std::move(columnDataEvaluators)}, srcNodeIDVector{nullptr},
          dstNodeIDVector{nullptr} {}
    EXPLICIT_COPY_DEFAULT_MOVE(RelInsertExecutor);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    // See comment in NodeInsertExecutor.
    void skipInsert();

private:
    RelInsertExecutor(const RelInsertExecutor& other);

    void writeResult();

private:
    storage::RelsStoreStats* relsStatistics;
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    std::vector<DataPos> columnVectorsPos;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnDataEvaluators;

    common::ValueVector* srcNodeIDVector;
    common::ValueVector* dstNodeIDVector;
    std::vector<common::ValueVector*> columnVectors;
    std::vector<common::ValueVector*> columnDataVectors;
};

} // namespace processor
} // namespace kuzu
