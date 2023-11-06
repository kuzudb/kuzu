#include "optimizer/filter_push_down_optimizer.h"

#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/scan/logical_dummy_scan.h"
#include "planner/operator/scan/logical_index_scan.h"
#include "planner/operator/scan/logical_scan_node_property.h"

using namespace kuzu::binder;
using namespace kuzu::common;
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

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitCrossProductReplace(
    std::shared_ptr<LogicalOperator> op) {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        auto optimizer = FilterPushDownOptimizer();
        op->setChild(i, optimizer.visitOperator(op->getChild(i)));
    }
    auto probeSchema = op->getChild(0)->getSchema();
    auto buildSchema = op->getChild(1)->getSchema();
    std::vector<join_condition_t> joinConditions;
    for (auto& predicate : predicateSet->equalityPredicates) {
        auto left = predicate->getChild(0);
        auto right = predicate->getChild(1);
        // TODO(Xiyang): this can only rewrite left = right, we should also be able to do
        // expr(left), expr(right)
        if (probeSchema->isExpressionInScope(*left) && buildSchema->isExpressionInScope(*right)) {
            joinConditions.emplace_back(left, right);
        } else if (probeSchema->isExpressionInScope(*right) &&
                   buildSchema->isExpressionInScope(*left)) {
            joinConditions.emplace_back(right, left);
        }
    }
    if (joinConditions.empty()) {
        return finishPushDown(op);
    }
    auto hashJoin = std::make_shared<LogicalHashJoin>(
        joinConditions, JoinType::INNER, op->getChild(0), op->getChild(1));
    hashJoin->setSIP(planner::SidewaysInfoPassing::PROHIBIT);
    hashJoin->computeFlatSchema();
    return hashJoin;
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::visitScanNodePropertyReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
    auto scan = (LogicalScanNodeProperty*)op.get();
    auto nodeID = scan->getNodeID();
    auto tableIDs = scan->getTableIDs();
    std::shared_ptr<Expression> primaryKeyEqualityComparison = nullptr;
    if (tableIDs.size() == 1) {
        primaryKeyEqualityComparison = predicateSet->popNodePKEqualityComparison(*nodeID);
    }
    if (primaryKeyEqualityComparison != nullptr) { // Try rewrite index scan
        auto rhs = primaryKeyEqualityComparison->getChild(1);
        if (rhs->expressionType == ExpressionType::LITERAL) {
            // Rewrite to index scan
            auto expressionsScan = std::make_shared<LogicalDummyScan>();
            expressionsScan->computeFlatSchema();
            std::vector<std::unique_ptr<LogicalIndexScanNodeInfo>> infos;
            KU_ASSERT(tableIDs.size() == 1);
            infos.push_back(std::make_unique<LogicalIndexScanNodeInfo>(
                tableIDs[0], nodeID, rhs, rhs->getDataType().copy()));
            auto indexScan = std::make_shared<LogicalIndexScanNode>(
                std::move(infos), std::move(expressionsScan));
            indexScan->computeFlatSchema();
            op->setChild(0, std::move(indexScan));
        } else {
            // Cannot rewrite and add predicate back.
            predicateSet->addPredicate(primaryKeyEqualityComparison);
        }
    }
    // Perform filter push down.
    auto currentRoot = scan->getChild(0);
    for (auto& predicate : predicateSet->equalityPredicates) {
        currentRoot = pushDownToScanNode(nodeID, tableIDs, predicate, currentRoot);
    }
    for (auto& predicate : predicateSet->nonEqualityPredicates) {
        currentRoot = pushDownToScanNode(nodeID, tableIDs, predicate, currentRoot);
    }
    // Scan remaining properties.
    expression_vector properties;
    for (auto& property : scan->getProperties()) {
        if (currentRoot->getSchema()->isExpressionInScope(*property)) {
            continue;
        }
        properties.push_back(property);
    }
    return appendScanNodeProperty(nodeID, tableIDs, properties, currentRoot);
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownOptimizer::pushDownToScanNode(
    std::shared_ptr<binder::Expression> nodeID, std::vector<common::table_id_t> tableIDs,
    std::shared_ptr<binder::Expression> predicate,
    std::shared_ptr<planner::LogicalOperator> child) {
    binder::expression_set propertiesSet;
    auto expressionCollector = std::make_unique<ExpressionCollector>();
    for (auto& expression : expressionCollector->collectPropertyExpressions(predicate)) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (child->getSchema()->isExpressionInScope(*propertyExpression)) {
            // Property already matched
            continue;
        }
        KU_ASSERT(propertyExpression->getVariableName() ==
                  ((PropertyExpression&)*nodeID).getVariableName());
        propertiesSet.insert(expression);
    }
    auto scanNodeProperty = appendScanNodeProperty(std::move(nodeID), std::move(tableIDs),
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
    std::shared_ptr<binder::Expression> nodeID, std::vector<common::table_id_t> nodeTableIDs,
    binder::expression_vector properties, std::shared_ptr<planner::LogicalOperator> child) {
    if (properties.empty()) {
        return child;
    }
    auto scanNodeProperty = std::make_shared<LogicalScanNodeProperty>(
        std::move(nodeID), std::move(nodeTableIDs), std::move(properties), std::move(child));
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
    if (predicate->expressionType == ExpressionType::EQUALS) {
        equalityPredicates.push_back(std::move(predicate));
    } else {
        nonEqualityPredicates.push_back(std::move(predicate));
    }
}

static bool isNodePrimaryKey(const Expression& expression, const Expression& nodeID) {
    if (expression.expressionType != ExpressionType::PROPERTY) {
        // not property
        return false;
    }
    auto& propertyExpression = (PropertyExpression&)expression;
    if (propertyExpression.getVariableName() != ((PropertyExpression&)nodeID).getVariableName()) {
        // not property for node
        return false;
    }
    return propertyExpression.isPrimaryKey();
}

std::shared_ptr<binder::Expression>
FilterPushDownOptimizer::PredicateSet::popNodePKEqualityComparison(
    const binder::Expression& nodeID) {
    // We pop when the first primary key equality comparison is found.
    auto resultPredicateIdx = INVALID_VECTOR_IDX;
    for (auto i = 0u; i < equalityPredicates.size(); ++i) {
        auto predicate = equalityPredicates[i];
        if (isNodePrimaryKey(*predicate->getChild(0), nodeID)) {
            resultPredicateIdx = i;
            break;
        } else if (isNodePrimaryKey(*predicate->getChild(1), nodeID)) {
            // Normalize primary key to LHS.
            auto leftChild = predicate->getChild(0);
            auto rightChild = predicate->getChild(1);
            predicate->setChild(1, leftChild);
            predicate->setChild(0, rightChild);
            resultPredicateIdx = i;
            break;
        }
    }
    if (resultPredicateIdx != INVALID_VECTOR_IDX) {
        auto result = equalityPredicates[resultPredicateIdx];
        equalityPredicates.erase(equalityPredicates.begin() + resultPredicateIdx);
        return result;
    }
    return nullptr;
}

} // namespace optimizer
} // namespace kuzu
