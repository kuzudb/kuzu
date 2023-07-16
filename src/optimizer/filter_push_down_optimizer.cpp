#include "optimizer/filter_push_down_optimizer.h"

#include "binder/expression/expression_visitor.h"
#include "binder/expression/property_expression.h"
#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void FilterPushDownOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator());
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::visitOperator(
    std::shared_ptr<planner::LogicalOperator> op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::FILTER: {
        return visitFilterReplace(op);
    }
    case LogicalOperatorType::CROSS_PRODUCT: {
        return visitCrossProductReplace(op);
    }
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        return visitScanNodePropertyReplace(op);
    }
    default: { // Stop current push down for unhandled operator.
        for (auto i = 0u; i < op->getNumChildren(); ++i) {
            // Start new push down for child.
            auto optimizer = FilterPushDownOptimizer();
            op->setChild(i, optimizer.visitOperator(op->getChild(i)));
        }
        op->computeFlatSchema();
        return finishPushDown(op);
    }
    }
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitFilterReplace(
    std::shared_ptr<LogicalOperator> op) {
    auto filter = (LogicalFilter*)op.get();
    predicateSet->addPredicate(filter->getPredicate());
    return visitOperator(filter->getChild(0));
}

// A trivial sub-plan is defined as a plan containing only simple table scan (i.e. SCAN_NODE,
// SCAN_NODE_PROPERTY), FILTER and PROJECTION.
static bool isTrivialSubPlan(LogicalOperator* root) {
    switch (root->getOperatorType()) {
    case LogicalOperatorType::FILTER:
    case LogicalOperatorType::SCAN_NODE_PROPERTY:
    case LogicalOperatorType::PROJECTION: { // operators we directly search through
        return isTrivialSubPlan(root->getChild(0).get());
    }
    case LogicalOperatorType::SCAN_NODE: {
        return true;
    }
    default:
        return false;
    }
}

static std::vector<std::shared_ptr<LogicalOperator>> fetchOpsInTrivialSubPlan(
    std::shared_ptr<LogicalOperator> root) {
    std::vector<std::shared_ptr<LogicalOperator>> result;
    auto op = std::move(root);
    while (op->getOperatorType() != LogicalOperatorType::SCAN_NODE) {
        result.push_back(op);
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    result.push_back(op);
    return result;
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitCrossProductReplace(
    std::shared_ptr<LogicalOperator> op) {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        auto optimizer = FilterPushDownOptimizer();
        op->setChild(i, optimizer.visitOperator(op->getChild(i)));
    }
    if (!isTrivialSubPlan(op->getChild(1).get())) {
        return finishPushDown(op);
    }
    auto buildOps = fetchOpsInTrivialSubPlan(op->getChild(1));
    auto node = ((LogicalScanNode&)*buildOps[buildOps.size() - 1]).getNode();
    auto primaryKeyEqualityComparison = predicateSet->popNodePKEqualityComparison(*node);
    if (primaryKeyEqualityComparison == nullptr) {
        return finishPushDown(op);
    }
    // Append index scan to left branch
    auto indexScan = make_shared<LogicalIndexScanNode>(
        node, primaryKeyEqualityComparison->getChild(1), op->getChild(0));
    indexScan->computeFlatSchema();
    // Append right branch (except for node table scan) to left branch
    buildOps[buildOps.size() - 2]->setChild(0, std::move(indexScan));
    for (auto i = 0; i < buildOps.size() - 1; ++i) {
        buildOps[i]->computeFlatSchema();
    }
    return buildOps[0];
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::visitScanNodePropertyReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
    auto scanNodeProperty = (LogicalScanNodeProperty*)op.get();
    assert(scanNodeProperty->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_NODE);
    auto node = ((LogicalScanNode&)*scanNodeProperty->getChild(0)).getNode();
    auto primaryKeyEqualityComparison = predicateSet->popNodePKEqualityComparison(*node);
    if (primaryKeyEqualityComparison != nullptr) { // Try rewrite index scan
        auto rhs = primaryKeyEqualityComparison->getChild(1);
        if (rhs->expressionType == common::ExpressionType::LITERAL) {
            // Rewrite to index scan
            auto expressionsScan = make_shared<LogicalExpressionsScan>(expression_vector{rhs});
            expressionsScan->computeFlatSchema();
            auto indexScan =
                std::make_shared<LogicalIndexScanNode>(node, rhs, std::move(expressionsScan));
            indexScan->computeFlatSchema();
            op->setChild(0, std::move(indexScan));
        } else {
            // Cannot rewrite and add predicate back.
            predicateSet->addPredicate(primaryKeyEqualityComparison);
        }
    }
    // Perform filter push down.
    auto currentRoot = scanNodeProperty->getChild(0);
    for (auto& predicate : predicateSet->equalityPredicates) {
        currentRoot = pushDownToScanNode(node, predicate, currentRoot);
    }
    for (auto& predicate : predicateSet->nonEqualityPredicates) {
        currentRoot = pushDownToScanNode(node, predicate, currentRoot);
    }
    // Scan remaining properties.
    expression_vector properties;
    for (auto& property : scanNodeProperty->getProperties()) {
        if (currentRoot->getSchema()->isExpressionInScope(*property)) {
            continue;
        }
        properties.push_back(property);
    }
    return appendScanNodeProperty(node, properties, currentRoot);
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::pushDownToScanNode(
    std::shared_ptr<binder::NodeExpression> node, std::shared_ptr<binder::Expression> predicate,
    std::shared_ptr<planner::LogicalOperator> child) {
    binder::expression_set propertiesSet;
    auto expressionCollector = std::make_unique<ExpressionCollector>();
    for (auto& expression : expressionCollector->collectPropertyExpressions(predicate)) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (child->getSchema()->isExpressionInScope(*propertyExpression)) {
            // Property already matched
            continue;
        }
        assert(propertyExpression->getVariableName() == node->getUniqueName());
        propertiesSet.insert(expression);
    }
    auto scanNodeProperty = appendScanNodeProperty(std::move(node),
        expression_vector{propertiesSet.begin(), propertiesSet.end()}, std::move(child));
    return appendFilter(std::move(predicate), scanNodeProperty);
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::finishPushDown(
    std::shared_ptr<planner::LogicalOperator> op) {
    if (predicateSet->isEmpty()) {
        return op;
    }
    auto currentRoot = op;
    for (auto& predicate : predicateSet->equalityPredicates) {
        currentRoot = appendFilter(predicate, currentRoot);
    }
    for (auto& predicate : predicateSet->nonEqualityPredicates) {
        currentRoot = appendFilter(predicate, currentRoot);
    }
    predicateSet->clear();
    return currentRoot;
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::appendScanNodeProperty(
    std::shared_ptr<binder::NodeExpression> node, binder::expression_vector properties,
    std::shared_ptr<planner::LogicalOperator> child) {
    if (properties.empty()) {
        return child;
    }
    auto scanNodeProperty = std::make_shared<LogicalScanNodeProperty>(
        std::move(node), std::move(properties), std::move(child));
    scanNodeProperty->computeFlatSchema();
    return scanNodeProperty;
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::appendFilter(
    std::shared_ptr<binder::Expression> predicate,
    std::shared_ptr<planner::LogicalOperator> child) {
    auto filter = std::make_shared<LogicalFilter>(std::move(predicate), std::move(child));
    filter->computeFlatSchema();
    return filter;
}

void FilterPushDownOptimizer::PredicateSet::addPredicate(
    std::shared_ptr<binder::Expression> predicate) {
    if (predicate->expressionType == common::ExpressionType::EQUALS) {
        equalityPredicates.push_back(std::move(predicate));
    } else {
        nonEqualityPredicates.push_back(std::move(predicate));
    }
}

static bool isNodePrimaryKey(const Expression& expression, const NodeExpression& node) {
    if (expression.expressionType != common::ExpressionType::PROPERTY) {
        // not property
        return false;
    }
    auto& propertyExpression = (PropertyExpression&)expression;
    if (propertyExpression.getVariableName() != node.getUniqueName()) {
        // not property for node
        return false;
    }
    return propertyExpression.isPrimaryKey();
}

std::shared_ptr<binder::Expression>
FilterPushDownOptimizer::PredicateSet::popNodePKEqualityComparison(
    const binder::NodeExpression& node) {
    if (node.isMultiLabeled()) { // Multi-labeled node scan can not be converted to index scan.
        return nullptr;
    }
    // We pop when the first primary key equality comparison is found.
    auto resultPredicateIdx = common::INVALID_VECTOR_IDX;
    for (auto i = 0u; i < equalityPredicates.size(); ++i) {
        auto predicate = equalityPredicates[i];
        if (isNodePrimaryKey(*predicate->getChild(0), node)) {
            resultPredicateIdx = i;
            break;
        } else if (isNodePrimaryKey(*predicate->getChild(1), node)) {
            // Normalize primary key to LHS.
            auto leftChild = predicate->getChild(0);
            auto rightChild = predicate->getChild(1);
            predicate->setChild(1, leftChild);
            predicate->setChild(0, rightChild);
            resultPredicateIdx = i;
            break;
        }
    }
    if (resultPredicateIdx != common::INVALID_VECTOR_IDX) {
        auto result = equalityPredicates[resultPredicateIdx];
        equalityPredicates.erase(equalityPredicates.begin() + resultPredicateIdx);
        return result;
    }
    return nullptr;
}

} // namespace optimizer
} // namespace kuzu
