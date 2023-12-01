#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace optimizer {

class FactorizationRewriter : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

    void visitOperator(planner::LogicalOperator* op);

private:
    void visitExtend(planner::LogicalOperator* op) override;
    void visitRecursiveExtend(planner::LogicalOperator* op) override;
    void visitHashJoin(planner::LogicalOperator* op) override;
    void visitIntersect(planner::LogicalOperator* op) override;
    void visitProjection(planner::LogicalOperator* op) override;
    void visitAccumulate(planner::LogicalOperator* op) override;
    void visitAggregate(planner::LogicalOperator* op) override;
    void visitOrderBy(planner::LogicalOperator* op) override;
    void visitLimit(planner::LogicalOperator* op) override;
    void visitDistinct(planner::LogicalOperator* op) override;
    void visitUnwind(planner::LogicalOperator* op) override;
    void visitUnion(planner::LogicalOperator* op) override;
    void visitFilter(planner::LogicalOperator* op) override;
    void visitSetNodeProperty(planner::LogicalOperator* op) override;
    void visitSetRelProperty(planner::LogicalOperator* op) override;
    void visitDeleteNode(planner::LogicalOperator* op) override;
    void visitDeleteRel(planner::LogicalOperator* op) override;
    void visitInsertNode(planner::LogicalOperator* op) override;
    void visitInsertRel(planner::LogicalOperator* op) override;
    void visitMerge(planner::LogicalOperator* op) override;
    void visitCopyTo(planner::LogicalOperator* op) override;

    std::shared_ptr<planner::LogicalOperator> appendFlattens(
        std::shared_ptr<planner::LogicalOperator> op,
        const std::unordered_set<planner::f_group_pos>& groupsPos);
    std::shared_ptr<planner::LogicalOperator> appendFlattenIfNecessary(
        std::shared_ptr<planner::LogicalOperator> op, planner::f_group_pos groupPos);
};

} // namespace optimizer
} // namespace kuzu
