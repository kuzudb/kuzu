#include "optimizer/correlated_subquery_unnest_solver.h"

#include "planner/operator/logical_hash_join.h"
#include "planner/operator/scan/logical_expressions_scan.h"
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void CorrelatedSubqueryUnnestSolver::solve(planner::LogicalOperator* root_) {
    visitOperator(root_);
}

void CorrelatedSubqueryUnnestSolver::visitOperator(LogicalOperator* op) {
    visitOperatorSwitch(op);
    if (LogicalOperatorUtils::isAccHashJoin(*op)) {
        solveAccHashJoin(op);
        return;
    }
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
}

void CorrelatedSubqueryUnnestSolver::solveAccHashJoin(LogicalOperator* op) const {
    auto& hashJoin = op->cast<LogicalHashJoin>();
    auto& sipInfo = hashJoin.getSIPInfoUnsafe();
    sipInfo.dependency = SIPDependency::BUILD_DEPENDS_ON_PROBE;
    sipInfo.direction = SIPDirection::PROBE_TO_BUILD;
    auto acc = op->getChild(0).get();
    auto rightSolver = std::make_unique<CorrelatedSubqueryUnnestSolver>(acc);
    rightSolver->solve(hashJoin.getChild(1).get());
    auto leftSolver = std::make_unique<CorrelatedSubqueryUnnestSolver>(nullptr);
    leftSolver->solve(acc->getChild(0).get());
}

void CorrelatedSubqueryUnnestSolver::visitExpressionsScan(LogicalOperator* op) {
    auto expressionsScan = op->ptrCast<LogicalExpressionsScan>();
    KU_ASSERT(accumulateOp != nullptr);
    expressionsScan->setOuterAccumulate(accumulateOp);
}

} // namespace optimizer
} // namespace kuzu
