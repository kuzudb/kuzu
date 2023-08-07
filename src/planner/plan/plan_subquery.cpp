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

static expression_vector getJoinNodeIDs(expression_vector& expressions) {
    expression_vector joinNodeIDs;
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
            joinNodeIDs.push_back(expression);
        }
    }
    return joinNodeIDs;
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
        appendCrossProduct(common::AccumulateType::OPTIONAL_, leftPlan, *rightPlan);
        return;
    }
    if (ExpressionUtil::allExpressionsHaveDataType(
            correlatedExpressions, LogicalTypeID::INTERNAL_ID)) {
        // All join conditions are internal IDs, unnest as left hash join.
        auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
        // Join nodes are scanned twice in both outer and inner. However, we make sure inner table
        // scan only scans node ID and does not scan from storage (i.e. no property scan).
        auto rightPlan =
            planQueryGraphCollectionInNewContext(joinNodeIDs, queryGraphCollection, predicates);
        appendHashJoin(joinNodeIDs, common::JoinType::LEFT, leftPlan, *rightPlan);
    } else {
        throw NotImplementedException("Correlated optional match is not supported.");
    }
}

void QueryPlanner::planRegularMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, LogicalPlan& leftPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, leftPlan.getSchema());
    auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
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
    // Multi-part query is actually CTE and CTE can be considered as a subquery but does not scan
    // from outer (i.e. can always be un-nest). So we plan multi-part query in the same way as
    // planning an un-nest subquery.
    auto rightPlan = planQueryGraphCollectionInNewContext(
        joinNodeIDs, queryGraphCollection, predicatesToPushDown);
    if (joinNodeIDs.empty()) {
        appendCrossProduct(common::AccumulateType::REGULAR, leftPlan, *rightPlan);
    } else {
        appendHashJoin(joinNodeIDs, common::JoinType::INNER, leftPlan, *rightPlan);
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
        throw NotImplementedException("Subquery is disconnected with outer query.");
    }
    if (ExpressionUtil::allExpressionsHaveDataType(
            correlatedExpressions, LogicalTypeID::INTERNAL_ID)) {
        auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
        // Unnest as mark join. See planOptionalMatch for unnesting logic.
        auto innerPlan = planQueryGraphCollectionInNewContext(
            joinNodeIDs, *subquery->getQueryGraphCollection(), predicates);
        appendMarkJoin(joinNodeIDs, expression, outerPlan, *innerPlan);
    } else {
        throw NotImplementedException("Correlated exists subquery is not supported.");
    }
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
