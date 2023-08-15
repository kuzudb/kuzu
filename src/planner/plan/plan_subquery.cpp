#include "binder/expression/existential_subquery_expression.h"
#include "binder/expression_visitor.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

static expression_vector getCorrelatedExpressions(const QueryGraphCollection& collection,
    const expression_vector& predicates, Schema* outerSchema) {
    expression_vector result;
    for (auto& predicate : predicates) {
        for (auto& expression : outerSchema->getSubExpressionsInScope(predicate)) {
            result.push_back(expression);
        }
    }
    for (auto& node : collection.getQueryNodes()) {
        if (outerSchema->isExpressionInScope(*node->getInternalIDProperty())) {
            result.push_back(node->getInternalIDProperty());
        }
    }
    return result;
}

void QueryPlanner::planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, LogicalPlan& leftPlan) {
    if (leftPlan.isEmpty()) {
        // Optional match is the first clause. No left plan to join.
        auto plan = planQueryGraphCollection(queryGraphCollection, predicates);
        leftPlan.setLastOperator(plan->getLastOperator());
        appendAccumulate(AccumulateType::OPTIONAL_, leftPlan);
        return;
    }
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, leftPlan.getSchema());
    if (correlatedExpressions.empty()) {
        // No join condition, apply cross product.
        auto rightPlan = planQueryGraphCollection(queryGraphCollection, predicates);
        appendCrossProduct(AccumulateType::OPTIONAL_, leftPlan, *rightPlan);
        return;
    }
    bool isInternalIDCorrelated = ExpressionUtil::isExpressionsWithDataType(
        correlatedExpressions, LogicalTypeID::INTERNAL_ID);
    std::unique_ptr<LogicalPlan> rightPlan;
    if (isInternalIDCorrelated) {
        // If all correlated expressions are node IDs. We can trivially unnest by scanning internal
        // ID in both outer and inner plan as these are fast in-memory operations. For node
        // properties, we only scan in the outer query.
        rightPlan = planQueryGraphCollectionInNewContext(SubqueryType::INTERNAL_ID_CORRELATED,
            correlatedExpressions, leftPlan.getCardinality(), queryGraphCollection, predicates);
    } else {
        // Unnest using ExpressionsScan which scans the accumulated table on probe side.
        rightPlan = planQueryGraphCollectionInNewContext(SubqueryType::CORRELATED,
            correlatedExpressions, leftPlan.getCardinality(), queryGraphCollection, predicates);
        appendAccumulate(AccumulateType::REGULAR, correlatedExpressions, leftPlan);
    }
    appendHashJoin(correlatedExpressions, JoinType::LEFT, leftPlan, *rightPlan);
}

void QueryPlanner::planRegularMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, LogicalPlan& leftPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, leftPlan.getSchema());
    expression_vector predicatesToPushDown, predicatesToPullUp;
    // E.g. MATCH (a) WITH COUNT(*) AS s MATCH (b) WHERE b.age > s
    // "b.age > s" should be pulled up after both MATCH clauses are joined.
    for (auto& predicate : predicates) {
        if (leftPlan.getSchema()->getSubExpressionsInScope(predicate).empty()) {
            predicatesToPushDown.push_back(predicate);
        } else {
            predicatesToPullUp.push_back(predicate);
        }
    }
    auto joinNodeIDs = ExpressionUtil::getExpressionsWithDataType(
        correlatedExpressions, LogicalTypeID::INTERNAL_ID);
    std::unique_ptr<LogicalPlan> rightPlan;
    if (joinNodeIDs.empty()) {
        rightPlan = planQueryGraphCollectionInNewContext(SubqueryType::NONE, correlatedExpressions,
            leftPlan.getCardinality(), queryGraphCollection, predicatesToPushDown);
        appendCrossProduct(AccumulateType::REGULAR, leftPlan, *rightPlan);
    } else {
        // TODO(Xiyang): there is a question regarding if we want to plan as a correlated subquery
        // Multi-part query is actually CTE and CTE can be considered as a subquery but does not
        // scan from outer.
        rightPlan = planQueryGraphCollectionInNewContext(SubqueryType::INTERNAL_ID_CORRELATED,
            joinNodeIDs, leftPlan.getCardinality(), queryGraphCollection, predicatesToPushDown);
        appendHashJoin(joinNodeIDs, JoinType::INNER, leftPlan, *rightPlan);
    }
    for (auto& predicate : predicatesToPullUp) {
        appendFilter(predicate, leftPlan);
    }
}

void QueryPlanner::planExistsSubquery(
    std::shared_ptr<Expression> expression, LogicalPlan& outerPlan) {
    assert(expression->expressionType == EXISTENTIAL_SUBQUERY);
    auto subquery = static_pointer_cast<ExistentialSubqueryExpression>(expression);
    auto predicates = subquery->getPredicatesSplitOnAnd();
    auto correlatedExpressions = outerPlan.getSchema()->getSubExpressionsInScope(subquery);
    if (correlatedExpressions.empty()) {
        throw common::NotImplementedException(
            "Exists subquery with no correlated join conditions is not yet supported.");
    }
    // See planOptionalMatch for un-nesting logic.
    bool isInternalIDCorrelated = ExpressionUtil::isExpressionsWithDataType(
        correlatedExpressions, LogicalTypeID::INTERNAL_ID);
    std::unique_ptr<LogicalPlan> innerPlan;
    if (isInternalIDCorrelated) {
        innerPlan = planQueryGraphCollectionInNewContext(SubqueryType::INTERNAL_ID_CORRELATED,
            correlatedExpressions, outerPlan.getCardinality(), *subquery->getQueryGraphCollection(),
            predicates);
    } else {
        appendAccumulate(AccumulateType::REGULAR, correlatedExpressions, outerPlan);
        innerPlan =
            planQueryGraphCollectionInNewContext(SubqueryType::CORRELATED, correlatedExpressions,
                outerPlan.getCardinality(), *subquery->getQueryGraphCollection(), predicates);
    }
    appendMarkJoin(correlatedExpressions, expression, outerPlan, *innerPlan);
}

void QueryPlanner::planSubqueryIfNecessary(
    const std::shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (ExpressionVisitor::hasSubqueryExpression(*expression)) {
        auto expressionCollector = std::make_unique<ExpressionCollector>();
        for (auto& expr : expressionCollector->collectTopLevelSubqueryExpressions(expression)) {
            planExistsSubquery(expr, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
