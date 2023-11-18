#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class NodeSetExecutor {
public:
    NodeSetExecutor(const DataPos& nodeIDPos, const DataPos& lhsVectorPos,
        std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : nodeIDPos{nodeIDPos}, lhsVectorPos{lhsVectorPos}, evaluator{std::move(evaluator)} {}
    virtual ~NodeSetExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void set(ExecutionContext* context) = 0;

    virtual std::unique_ptr<NodeSetExecutor> copy() const = 0;

    static std::vector<std::unique_ptr<NodeSetExecutor>> copy(
        const std::vector<std::unique_ptr<NodeSetExecutor>>& executors);

protected:
    DataPos nodeIDPos;
    DataPos lhsVectorPos;
    std::unique_ptr<evaluator::ExpressionEvaluator> evaluator;

    common::ValueVector* nodeIDVector = nullptr;
    // E.g. SET a.age = b.age; a.age is the left-hand-side (lhs) and b.age is the right-hand-side
    // (rhs)
    common::ValueVector* lhsVector = nullptr;
    common::ValueVector* rhsVector = nullptr;
};

struct NodeSetInfo {
    storage::NodeTable* table;
    common::column_id_t columnID;
};

class SingleLabelNodeSetExecutor final : public NodeSetExecutor {
public:
    SingleLabelNodeSetExecutor(NodeSetInfo setInfo, const DataPos& nodeIDPos,
        const DataPos& lhsVectorPos, std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : NodeSetExecutor{nodeIDPos, lhsVectorPos, std::move(evaluator)}, setInfo{setInfo} {}

    SingleLabelNodeSetExecutor(const SingleLabelNodeSetExecutor& other)
        : NodeSetExecutor{other.nodeIDPos, other.lhsVectorPos, other.evaluator->clone()},
          setInfo(other.setInfo) {}

    void set(ExecutionContext* context) override;

    inline std::unique_ptr<NodeSetExecutor> copy() const override {
        return std::make_unique<SingleLabelNodeSetExecutor>(*this);
    }

private:
    NodeSetInfo setInfo;
};

class MultiLabelNodeSetExecutor final : public NodeSetExecutor {
public:
    MultiLabelNodeSetExecutor(std::unordered_map<common::table_id_t, NodeSetInfo> tableIDToSetInfo,
        const DataPos& nodeIDPos, const DataPos& lhsVectorPos,
        std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : NodeSetExecutor{nodeIDPos, lhsVectorPos, std::move(evaluator)},
          tableIDToSetInfo{std::move(tableIDToSetInfo)} {}
    MultiLabelNodeSetExecutor(const MultiLabelNodeSetExecutor& other)
        : NodeSetExecutor{other.nodeIDPos, other.lhsVectorPos, other.evaluator->clone()},
          tableIDToSetInfo{other.tableIDToSetInfo} {}

    void set(ExecutionContext* context) override;

    inline std::unique_ptr<NodeSetExecutor> copy() const override {
        return std::make_unique<MultiLabelNodeSetExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, NodeSetInfo> tableIDToSetInfo;
};

class RelSetExecutor {
public:
    RelSetExecutor(const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos,
        const DataPos& relIDPos, const DataPos& lhsVectorPos,
        std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : srcNodeIDPos{srcNodeIDPos}, dstNodeIDPos{dstNodeIDPos}, relIDPos{relIDPos},
          lhsVectorPos{lhsVectorPos}, evaluator{std::move(evaluator)} {}
    virtual ~RelSetExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void set(ExecutionContext* context) = 0;

    virtual std::unique_ptr<RelSetExecutor> copy() const = 0;

    static std::vector<std::unique_ptr<RelSetExecutor>> copy(
        const std::vector<std::unique_ptr<RelSetExecutor>>& executors);

protected:
    DataPos srcNodeIDPos;
    DataPos dstNodeIDPos;
    DataPos relIDPos;
    DataPos lhsVectorPos;
    std::unique_ptr<evaluator::ExpressionEvaluator> evaluator;

    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    common::ValueVector* relIDVector = nullptr;
    // See NodeSetExecutor for an example for lhsVector & rhsVector.
    common::ValueVector* lhsVector = nullptr;
    common::ValueVector* rhsVector = nullptr;
};

class SingleLabelRelSetExecutor final : public RelSetExecutor {
public:
    SingleLabelRelSetExecutor(storage::RelTable* table, common::column_id_t columnID,
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos,
        const DataPos& lhsVectorPos, std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : RelSetExecutor{srcNodeIDPos, dstNodeIDPos, relIDPos, lhsVectorPos, std::move(evaluator)},
          table{table}, columnID{columnID} {}
    SingleLabelRelSetExecutor(const SingleLabelRelSetExecutor& other)
        : RelSetExecutor{other.srcNodeIDPos, other.dstNodeIDPos, other.relIDPos, other.lhsVectorPos,
              other.evaluator->clone()},
          table{other.table}, columnID{other.columnID} {}

    void set(ExecutionContext* context) override;

    inline std::unique_ptr<RelSetExecutor> copy() const override {
        return std::make_unique<SingleLabelRelSetExecutor>(*this);
    }

private:
    storage::RelTable* table;
    common::column_id_t columnID;
};

class MultiLabelRelSetExecutor final : public RelSetExecutor {
public:
    MultiLabelRelSetExecutor(
        std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::column_id_t>>
            tableIDToTableAndColumnID,
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos,
        const DataPos& lhsVectorPos, std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : RelSetExecutor{srcNodeIDPos, dstNodeIDPos, relIDPos, lhsVectorPos, std::move(evaluator)},
          tableIDToTableAndColumnID{std::move(tableIDToTableAndColumnID)} {}
    MultiLabelRelSetExecutor(const MultiLabelRelSetExecutor& other)
        : RelSetExecutor{other.srcNodeIDPos, other.dstNodeIDPos, other.relIDPos, other.lhsVectorPos,
              other.evaluator->clone()},
          tableIDToTableAndColumnID{other.tableIDToTableAndColumnID} {}

    void set(ExecutionContext* context) override;

    inline std::unique_ptr<RelSetExecutor> copy() const override {
        return std::make_unique<MultiLabelRelSetExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::column_id_t>>
        tableIDToTableAndColumnID;
};

} // namespace processor
} // namespace kuzu
