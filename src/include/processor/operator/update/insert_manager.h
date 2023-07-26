#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// TODO(Guodong): the following class should be moved to storage.
class NodeTableInsertManager {
public:
    NodeTableInsertManager(storage::NodeTable* table,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator,
        std::vector<storage::RelTable*> relTablesToInit, const DataPos& outNodeIDVectorPos)
        : table{table}, primaryKeyEvaluator{std::move(primaryKeyEvaluator)},
          relTablesToInit{std::move(relTablesToInit)}, outNodeIDVectorPos{outNodeIDVectorPos} {}
    NodeTableInsertManager(const NodeTableInsertManager& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<NodeTableInsertManager> copy() {
        return std::make_unique<NodeTableInsertManager>(*this);
    }

private:
    storage::NodeTable* table;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> primaryKeyEvaluator;
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos outNodeIDVectorPos;

    common::ValueVector* primaryKeyVector = nullptr;
    common::ValueVector* outNodeIDVector = nullptr;
};

class RelTableInsertManager {
public:
    RelTableInsertManager(storage::RelsStatistics& relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos, common::table_id_t srcNodeTableID,
        common::table_id_t dstNodeTableID,
        std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, srcNodeTableID{srcNodeTableID}, dstNodeTableID{dstNodeTableID},
          evaluators{std::move(evaluators)} {}
    RelTableInsertManager(const RelTableInsertManager& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<RelTableInsertManager> copy() {
        return std::make_unique<RelTableInsertManager>(*this);
    }

private:
    storage::RelsStatistics& relsStatistics;
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    common::table_id_t srcNodeTableID;
    common::table_id_t dstNodeTableID;
    std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> evaluators;

    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    std::vector<common::ValueVector*> propertyVectors;
};

} // namespace processor
} // namespace kuzu
