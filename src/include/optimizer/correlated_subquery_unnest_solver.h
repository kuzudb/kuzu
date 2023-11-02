#pragma once

#include "logical_operator_visitor.h"

namespace kuzu {
namespace optimizer {

class CorrelatedSubqueryUnnestSolver : public LogicalOperatorVisitor {
public:
    CorrelatedSubqueryUnnestSolver(planner::LogicalOperator* accumulateOp)
        : accumulateOp{accumulateOp} {}
    void solve(planner::LogicalOperator* root_);

private:
    void visitOperator(planner::LogicalOperator* op);
    void visitExpressionsScan(planner::LogicalOperator* op) final;

    inline bool isAccHashJoin(planner::LogicalOperator* op) const {
        return op->getOperatorType() == planner::LogicalOperatorType::HASH_JOIN &&
               op->getChild(0)->getOperatorType() == planner::LogicalOperatorType::ACCUMULATE;
    }
    void solveAccHashJoin(planner::LogicalOperator* op) const;

private:
    planner::LogicalOperator* accumulateOp;
};

} // namespace optimizer
} // namespace kuzu
