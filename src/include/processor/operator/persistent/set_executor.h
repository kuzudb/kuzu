#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct NodeSetInfo {
    DataPos nodeIDPos;
    DataPos lhsPos;
    DataPos pkPos = DataPos::getInvalidPos();

    NodeSetInfo(DataPos nodeIDPos, DataPos lhsPos) : nodeIDPos{nodeIDPos}, lhsPos{lhsPos} {}
    EXPLICIT_COPY_DEFAULT_MOVE(NodeSetInfo);

private:
    NodeSetInfo(const NodeSetInfo& other)
        : nodeIDPos{other.nodeIDPos}, lhsPos{other.lhsPos}, pkPos{other.pkPos} {}
};

class NodeSetExecutor {
public:
    NodeSetExecutor(NodeSetInfo info, std::unique_ptr<evaluator::ExpressionEvaluator> evaluator)
        : info{std::move(info)}, evaluator{std::move(evaluator)} {}
    NodeSetExecutor(const NodeSetExecutor& other)
        : info{other.info.copy()}, evaluator{other.evaluator->clone()} {}
    virtual ~NodeSetExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void set(ExecutionContext* context) = 0;

    virtual std::unique_ptr<NodeSetExecutor> copy() const = 0;

    static std::vector<std::unique_ptr<NodeSetExecutor>> copy(
        const std::vector<std::unique_ptr<NodeSetExecutor>>& others);

protected:
    NodeSetInfo info;
    std::unique_ptr<evaluator::ExpressionEvaluator> evaluator;

    common::ValueVector* nodeIDVector = nullptr;
    // E.g. SET a.age = b.age; a.age is the left-hand-side (lhs) and b.age is the right-hand-side
    // (rhs)
    common::ValueVector* lhsVector = nullptr;
    common::ValueVector* rhsVector = nullptr;
    common::ValueVector* pkVector = nullptr;
};

struct ExtraNodeSetInfo {
    storage::NodeTable* table;
    common::column_id_t columnID;

    ExtraNodeSetInfo(storage::NodeTable* table, common::column_id_t columnID)
        : table{table}, columnID{columnID} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ExtraNodeSetInfo);

private:
    ExtraNodeSetInfo(const ExtraNodeSetInfo& other)
        : table{other.table}, columnID{other.columnID} {}
};

class SingleLabelNodeSetExecutor final : public NodeSetExecutor {
public:
    SingleLabelNodeSetExecutor(NodeSetInfo setInfo,
        std::unique_ptr<evaluator::ExpressionEvaluator> evaluator, ExtraNodeSetInfo extraInfo)
        : NodeSetExecutor{std::move(setInfo), std::move(evaluator)},
          extraInfo{std::move(extraInfo)} {}

    SingleLabelNodeSetExecutor(const SingleLabelNodeSetExecutor& other)
        : NodeSetExecutor{other}, extraInfo(other.extraInfo.copy()) {}

    void set(ExecutionContext* context) override;

    std::unique_ptr<NodeSetExecutor> copy() const override {
        return std::make_unique<SingleLabelNodeSetExecutor>(*this);
    }

private:
    ExtraNodeSetInfo extraInfo;
};

class MultiLabelNodeSetExecutor final : public NodeSetExecutor {
public:
    MultiLabelNodeSetExecutor(NodeSetInfo info,
        std::unique_ptr<evaluator::ExpressionEvaluator> evaluator,
        common::table_id_map_t<ExtraNodeSetInfo> extraInfos)
        : NodeSetExecutor{std::move(info), std::move(evaluator)},
          extraInfos{std::move(extraInfos)} {}
    MultiLabelNodeSetExecutor(const MultiLabelNodeSetExecutor& other)
        : NodeSetExecutor{other}, extraInfos{copyMap(other.extraInfos)} {}

    void set(ExecutionContext* context) override;

    std::unique_ptr<NodeSetExecutor> copy() const override {
        return std::make_unique<MultiLabelNodeSetExecutor>(*this);
    }

private:
    common::table_id_map_t<ExtraNodeSetInfo> extraInfos;
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

    std::unique_ptr<RelSetExecutor> copy() const override {
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

    std::unique_ptr<RelSetExecutor> copy() const override {
        return std::make_unique<MultiLabelRelSetExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, std::pair<storage::RelTable*, common::column_id_t>>
        tableIDToTableAndColumnID;
};

} // namespace processor
} // namespace kuzu
