#include "optimizer/index_nested_loop_join_optimizer.h"

#include "binder/expression/property_expression.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void IndexNestedLoopJoinOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator());
}

std::shared_ptr<planner::LogicalOperator> IndexNestedLoopJoinOptimizer::visitOperator(
    std::shared_ptr<planner::LogicalOperator> op) {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    return visitOperatorReplaceSwitch(op);
}

static bool isPrimaryKey(const binder::Expression& expression) {
    if (expression.expressionType != common::PROPERTY) {
        return false;
    }
    return ((PropertyExpression&)expression).isPrimaryKey();
}

static bool isPrimaryKeyEqualityComparison(const Expression& expression) {
    if (expression.expressionType != common::EQUALS) {
        return false;
    }
    auto left = expression.getChild(0);
    auto right = expression.getChild(1);
    if (isPrimaryKey(*left) && isPrimaryKey(*right)) {
        return true;
    }
    return false;
}

std::shared_ptr<LogicalOperator> IndexNestedLoopJoinOptimizer::visitFilterReplace(
    std::shared_ptr<LogicalOperator> op) {
    // Match filter on primary key
    auto filter = (LogicalFilter*)op.get();
    auto predicate = filter->getPredicate();
    if (!isPrimaryKeyEqualityComparison(*predicate)) { // no need to rewrite
        return op;
    }
    auto currentOp = op->getChild(0);
    // Match cross product
    if (currentOp->getOperatorType() != LogicalOperatorType::CROSS_PRODUCT) {
        return op;
    }
    // Rewrite cross product
    return rewriteCrossProduct(currentOp, predicate);
}

std::shared_ptr<LogicalOperator> IndexNestedLoopJoinOptimizer::rewriteCrossProduct(
    std::shared_ptr<LogicalOperator> op, std::shared_ptr<Expression> predicate) {
    auto leftScan = (LogicalScanNode*)searchScanNodeOnPipeline(op->getChild(0).get());
    auto rightScan = (LogicalScanNode*)searchScanNodeOnPipeline(op->getChild(1).get());
    auto leftPrimaryKey = static_pointer_cast<PropertyExpression>(predicate->getChild(0));
    auto rightPrimaryKey = static_pointer_cast<PropertyExpression>(predicate->getChild(1));
    std::shared_ptr<PropertyExpression> primaryKey = nullptr;
    if (leftScan->getNode()->getUniqueName() == leftPrimaryKey->getVariableName() &&
        rightScan->getNode()->getUniqueName() == rightPrimaryKey->getVariableName()) {
        primaryKey = leftPrimaryKey;
    } else if (leftScan->getNode()->getUniqueName() == rightPrimaryKey->getVariableName() &&
               rightScan->getNode()->getUniqueName() == leftPrimaryKey->getVariableName()) {
        primaryKey = rightPrimaryKey;
    } else {
        return op;
    }
    // Append index scan to left branch
    auto indexScan =
        make_shared<LogicalIndexScanNode>(rightScan->getNode(), primaryKey, op->getChild(0));
    // Append right branch (except for node table scan) to left branch
    auto rightOp = op->getChild(1);
    while (rightOp->getNumChildren() != 0) {
        if (rightOp->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_NODE) {
            rightOp->setChild(0, std::move(indexScan));
            break;
        }
        rightOp = rightOp->getChild(0);
    }
    auto newRoot = op->getChild(1);
    return newRoot;
}

LogicalOperator* IndexNestedLoopJoinOptimizer::searchScanNodeOnPipeline(
    planner::LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FILTER:
    case LogicalOperatorType::SCAN_NODE_PROPERTY:
    case LogicalOperatorType::PROJECTION: { // operators we directly search through
        return searchScanNodeOnPipeline(op->getChild(0).get());
    }
    case LogicalOperatorType::SCAN_NODE: {
        return op;
    }
    default: // search failed for unhandled operator
        return nullptr;
    }
}

} // namespace optimizer
} // namespace kuzu
