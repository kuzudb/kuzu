#pragma once

#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

// ProjectionPushDownOptimizer implements the logic to avoid materializing unnecessary properties
// for hash join build.
// Note the optimization is for properties only but not for general expressions. This is because
// it's hard to figure out what expression is in-use, e.g. COUNT(a.age) + 1, it could be either the
// whole expression was evaluated in a WITH clause or only COUNT(a.age) was evaluated or only a.age
// is evaluate. For simplicity, we only consider the push down for property.
class ProjectionPushDownOptimizer {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);

    void visitAccumulate(planner::LogicalOperator* op);
    void visitFilter(planner::LogicalOperator* op);
    void visitHashJoin(planner::LogicalOperator* op);
    void visitIntersect(planner::LogicalOperator* op);
    void visitProjection(planner::LogicalOperator* op);
    void visitOrderBy(planner::LogicalOperator* op);
    void visitUnwind(planner::LogicalOperator* op);
    void visitSetNodeProperty(planner::LogicalOperator* op);
    void visitSetRelProperty(planner::LogicalOperator* op);
    void visitCreateNode(planner::LogicalOperator* op);
    void visitCreateRel(planner::LogicalOperator* op);
    void visitDeleteNode(planner::LogicalOperator* op);
    void visitDeleteRel(planner::LogicalOperator* op);

    void collectPropertiesInUse(std::shared_ptr<binder::Expression> expression);

    binder::expression_vector pruneExpressions(const binder::expression_vector& expressions);

private:
    binder::expression_set propertiesInUse;
};

} // namespace optimizer
} // namespace kuzu
