#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class NodeSetExecutor {
public:
    NodeSetExecutor(
        const DataPos& nodeIDPos, std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : nodeIDPos{nodeIDPos}, evaluator{std::move(evaluator)} {}
    virtual ~NodeSetExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void set() = 0;

    virtual std::unique_ptr<NodeSetExecutor> copy() const = 0;

protected:
    DataPos nodeIDPos;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;

    common::ValueVector* nodeIDVector;
    common::ValueVector* rhsVector;
};

class SingleLabelNodeSetExecutor : public NodeSetExecutor {
public:
    SingleLabelNodeSetExecutor(storage::NodeColumn* column, const DataPos& nodeIDPos,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : NodeSetExecutor{nodeIDPos, std::move(evaluator)}, column{column} {}
    SingleLabelNodeSetExecutor(const SingleLabelNodeSetExecutor& other)
        : NodeSetExecutor{other.nodeIDPos, other.evaluator->clone()}, column{other.column} {}

    void set() final;

    inline std::unique_ptr<NodeSetExecutor> copy() const final {
        return std::make_unique<SingleLabelNodeSetExecutor>(*this);
    }

private:
    storage::NodeColumn* column;
};

class MultiLabelNodeSetExecutor : public NodeSetExecutor {
public:
    MultiLabelNodeSetExecutor(
        std::unordered_map<common::table_id_t, storage::NodeColumn*> tableIDToColumn,
        const DataPos& nodeIDPos, std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : NodeSetExecutor{nodeIDPos, std::move(evaluator)}, tableIDToColumn{
                                                                std::move(tableIDToColumn)} {}
    MultiLabelNodeSetExecutor(const MultiLabelNodeSetExecutor& other)
        : NodeSetExecutor{other.nodeIDPos, other.evaluator->clone()}, tableIDToColumn{
                                                                          other.tableIDToColumn} {}

    void set() final;

    inline std::unique_ptr<NodeSetExecutor> copy() const final {
        return std::make_unique<MultiLabelNodeSetExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, storage::NodeColumn*> tableIDToColumn;
};

class RelSetExecutor {
public:
    RelSetExecutor(const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos,
        const DataPos& relIDPos, std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : srcNodeIDPos{srcNodeIDPos},
          dstNodeIDPos{dstNodeIDPos}, relIDPos{relIDPos}, evaluator{std::move(evaluator)} {}
    virtual ~RelSetExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void set() = 0;

    virtual std::unique_ptr<RelSetExecutor> copy() const = 0;

protected:
    DataPos srcNodeIDPos;
    DataPos dstNodeIDPos;
    DataPos relIDPos;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;

    common::ValueVector* srcNodeIDVector;
    common::ValueVector* dstNodeIDVector;
    common::ValueVector* relIDVector;
    common::ValueVector* rhsVector;
};

class SingleLabelRelSetExecutor : public RelSetExecutor {
public:
    SingleLabelRelSetExecutor(storage::RelTable* table, common::property_id_t propertyID,
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : RelSetExecutor{srcNodeIDPos, dstNodeIDPos, relIDPos, std::move(evaluator)}, table{table},
          propertyID{propertyID} {}
    SingleLabelRelSetExecutor(const SingleLabelRelSetExecutor& other)
        : RelSetExecutor{other.srcNodeIDPos, other.dstNodeIDPos, other.relIDPos,
              other.evaluator->clone()},
          table{other.table}, propertyID{other.propertyID} {}

    void set() final;

    inline std::unique_ptr<RelSetExecutor> copy() const final {
        return std::make_unique<SingleLabelRelSetExecutor>(*this);
    }

private:
    storage::RelTable* table;
    common::property_id_t propertyID;
};

class MultiLabelRelSetExecutor : public RelSetExecutor {
public:
    MultiLabelRelSetExecutor(
        std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::property_id_t>>
            tableIDToTableAndPropertyID,
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : RelSetExecutor{srcNodeIDPos, dstNodeIDPos, relIDPos, std::move(evaluator)},
          tableIDToTableAndPropertyID{std::move(tableIDToTableAndPropertyID)} {}
    MultiLabelRelSetExecutor(const MultiLabelRelSetExecutor& other)
        : RelSetExecutor{other.srcNodeIDPos, other.dstNodeIDPos, other.relIDPos,
              other.evaluator->clone()},
          tableIDToTableAndPropertyID{other.tableIDToTableAndPropertyID} {}

    void set() final;

    inline std::unique_ptr<RelSetExecutor> copy() const final {
        return std::make_unique<MultiLabelRelSetExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::property_id_t>>
        tableIDToTableAndPropertyID;
};

} // namespace processor
} // namespace kuzu
