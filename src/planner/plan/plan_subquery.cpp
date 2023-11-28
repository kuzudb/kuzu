#include "binder/expression/expression_util.h"
#include "binder/expression/subquery_expression.h"
#include "binder/expression_visitor.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;
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
        if (outerSchema->isExpressionInScope(*node->getInternalID())) {
            result.push_back(node->getInternalID());
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

void QueryPlanner::planSubquery(
    const std::shared_ptr<Expression>& expression, LogicalPlan& outerPlan) {
    KU_ASSERT(expression->expressionType == ExpressionType::SUBQUERY);
    auto subquery = static_pointer_cast<SubqueryExpression>(expression);
    auto predicates = subquery->getPredicatesSplitOnAnd();
    auto correlatedExprs = outerPlan.getSchema()->getSubExpressionsInScope(expression);
    std::unique_ptr<LogicalPlan> innerPlan;
    if (correlatedExprs.empty()) {
        innerPlan = planQueryGraphCollectionInNewContext(SubqueryType::NONE, correlatedExprs,
            outerPlan.getCardinality(), *subquery->getQueryGraphCollection(), predicates);
        switch (subquery->getSubqueryType()) {
        case common::SubqueryType::EXISTS: {
            appendAggregate(
                expression_vector{}, expression_vector{subquery->getCountStarExpr()}, *innerPlan);
            appendProjection(expression_vector{subquery->getProjectionExpr()}, *innerPlan);
        } break;
        case common::SubqueryType::COUNT: {
            appendAggregate(
                expression_vector{}, expression_vector{subquery->getProjectionExpr()}, *innerPlan);
        } break;
        default:
            KU_UNREACHABLE;
        }
        appendCrossProduct(AccumulateType::REGULAR, outerPlan, *innerPlan);
    } else {
        auto isInternalIDCorrelated =
            ExpressionUtil::isExpressionsWithDataType(correlatedExprs, LogicalTypeID::INTERNAL_ID);
        if (isInternalIDCorrelated) {
            innerPlan = planQueryGraphCollectionInNewContext(SubqueryType::INTERNAL_ID_CORRELATED,
                correlatedExprs, outerPlan.getCardinality(), *subquery->getQueryGraphCollection(),
                predicates);
        } else {
            innerPlan =
                planQueryGraphCollectionInNewContext(SubqueryType::CORRELATED, correlatedExprs,
                    outerPlan.getCardinality(), *subquery->getQueryGraphCollection(), predicates);
            appendAccumulate(AccumulateType::REGULAR, correlatedExprs, outerPlan);
        }
        switch (subquery->getSubqueryType()) {
        case common::SubqueryType::EXISTS: {
            appendMarkJoin(correlatedExprs, expression, outerPlan, *innerPlan);
        } break;
        case common::SubqueryType::COUNT: {
            appendAggregate(
                correlatedExprs, expression_vector{subquery->getProjectionExpr()}, *innerPlan);
            appendHashJoin(correlatedExprs, common::JoinType::COUNT, outerPlan, *innerPlan);
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
}

void QueryPlanner::planSubqueryIfNecessary(
    const std::shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (ExpressionVisitor::hasSubquery(*expression)) {
        auto expressionCollector = std::make_unique<ExpressionCollector>();
        for (auto& expr : expressionCollector->collectTopLevelSubqueryExpressions(expression)) {
            if (plan.getSchema()->isExpressionInScope(*expr)) {
                continue;
            }
            planSubquery(expr, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
