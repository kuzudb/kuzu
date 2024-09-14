#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/subquery_expression.h"
#include "binder/expression_visitor.h"
#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

static binder::expression_vector getDependentExprs(std::shared_ptr<Expression> expr,
    const Schema& schema) {
    auto analyzer = GroupDependencyAnalyzer(true /* collectDependentExpr */, schema);
    analyzer.visit(expr);
    return analyzer.getDependentExprs();
}

binder::expression_vector Planner::getCorrelatedExprs(const QueryGraphCollection& collection,
    const expression_vector& predicates, Schema* outerSchema) {
    expression_vector result;
    for (auto& predicate : predicates) {
        for (auto& expression : getDependentExprs(predicate, *outerSchema)) {
            result.push_back(expression);
        }
    }
    for (auto& node : collection.getQueryNodes()) {
        if (outerSchema->isExpressionInScope(*node->getInternalID())) {
            result.push_back(node->getInternalID());
        }
    }
    return ExpressionUtil::removeDuplication(result);
}

void Planner::planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, const binder::expression_vector& corrExprs,
    LogicalPlan& leftPlan) {
    planOptionalMatch(queryGraphCollection, predicates, corrExprs, nullptr /* mark */, leftPlan);
}

static bool isInternalIDCorrelated(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& exprs) {
    for (auto& expr : exprs) {
        if (expr->getDataType().getLogicalTypeID() != LogicalTypeID::INTERNAL_ID) {
            return false;
        }
        // Internal ID might be collected from exists subquery so we need to further check if
        // it is in query graph.
        if (!queryGraphCollection.contains(
                expr->constCast<PropertyExpression>().getVariableName())) {
            return false;
        }
    }
    return true;
}

void Planner::planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, const binder::expression_vector& corrExprs,
    std::shared_ptr<Expression> mark, LogicalPlan& leftPlan) {
    auto info = QueryGraphPlanningInfo();
    info.predicates = predicates;
    if (leftPlan.isEmpty()) {
        // Optional match is the first clause. No left plan to join.
        auto plan = planQueryGraphCollection(queryGraphCollection, info);
        leftPlan.setLastOperator(plan->getLastOperator());
        appendOptionalAccumulate(mark, leftPlan);
        return;
    }
    if (corrExprs.empty()) {
        // No join condition, apply cross product.
        auto rightPlan = planQueryGraphCollection(queryGraphCollection, info);
        if (leftPlan.hasUpdate()) {
            appendAccOptionalCrossProduct(mark, leftPlan, *rightPlan, leftPlan);
        } else {
            appendOptionalCrossProduct(mark, leftPlan, *rightPlan, leftPlan);
        }
        return;
    }
    info.corrExprs = corrExprs;
    info.corrExprsCard = leftPlan.getCardinality();
    std::unique_ptr<LogicalPlan> rightPlan;
    if (isInternalIDCorrelated(queryGraphCollection, corrExprs)) {
        // If all correlated expressions are node IDs. We can trivially unnest by scanning internal
        // ID in both outer and inner plan as these are fast in-memory operations. For node
        // properties, we only scan in the outer query.
        info.subqueryType = SubqueryType::INTERNAL_ID_CORRELATED;
        rightPlan = planQueryGraphCollectionInNewContext(queryGraphCollection, info);
    } else {
        // Unnest using ExpressionsScan which scans the accumulated table on probe side.
        info.subqueryType = SubqueryType::CORRELATED;
        rightPlan = planQueryGraphCollectionInNewContext(queryGraphCollection, info);
        appendAccumulate(corrExprs, leftPlan);
    }
    if (leftPlan.hasUpdate()) {
        appendAccHashJoin(corrExprs, JoinType::LEFT, mark, leftPlan, *rightPlan, leftPlan);
    } else {
        appendHashJoin(corrExprs, JoinType::LEFT, mark, leftPlan, *rightPlan, leftPlan);
    }
}

void Planner::planRegularMatch(const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates, LogicalPlan& leftPlan) {
    expression_vector predicatesToPushDown, predicatesToPullUp;
    // E.g. MATCH (a) WITH COUNT(*) AS s MATCH (b) WHERE b.age > s
    // "b.age > s" should be pulled up after both MATCH clauses are joined.
    for (auto& predicate : predicates) {
        if (getDependentExprs(predicate, *leftPlan.getSchema()).empty()) {
            predicatesToPushDown.push_back(predicate);
        } else {
            predicatesToPullUp.push_back(predicate);
        }
    }
    auto correlatedExpressions =
        getCorrelatedExprs(queryGraphCollection, predicatesToPushDown, leftPlan.getSchema());
    auto joinNodeIDs = ExpressionUtil::getExpressionsWithDataType(correlatedExpressions,
        LogicalTypeID::INTERNAL_ID);
    auto info = QueryGraphPlanningInfo();
    info.predicates = predicatesToPushDown;
    if (joinNodeIDs.empty()) {
        info.subqueryType = SubqueryType::NONE;
        auto rightPlan = planQueryGraphCollectionInNewContext(queryGraphCollection, info);
        if (leftPlan.hasUpdate()) {
            appendCrossProduct(*rightPlan, leftPlan, leftPlan);
        } else {
            appendCrossProduct(leftPlan, *rightPlan, leftPlan);
        }
    } else {
        // TODO(Xiyang): there is a question regarding if we want to plan as a correlated subquery
        // Multi-part query is actually CTE and CTE can be considered as a subquery but does not
        // scan from outer.
        info.subqueryType = SubqueryType::INTERNAL_ID_CORRELATED;
        info.corrExprs = joinNodeIDs;
        info.corrExprsCard = leftPlan.getCardinality();
        auto rightPlan = planQueryGraphCollectionInNewContext(queryGraphCollection, info);
        if (leftPlan.hasUpdate()) {
            appendHashJoin(joinNodeIDs, JoinType::INNER, *rightPlan, leftPlan, leftPlan);
        } else {
            appendHashJoin(joinNodeIDs, JoinType::INNER, leftPlan, *rightPlan, leftPlan);
        }
    }
    for (auto& predicate : predicatesToPullUp) {
        appendFilter(predicate, leftPlan);
    }
}

void Planner::planSubquery(const std::shared_ptr<Expression>& expression, LogicalPlan& outerPlan) {
    KU_ASSERT(expression->expressionType == ExpressionType::SUBQUERY);
    auto subquery = static_pointer_cast<SubqueryExpression>(expression);
    auto predicates = subquery->getPredicatesSplitOnAnd();
    auto correlatedExprs = getDependentExprs(expression, *outerPlan.getSchema());
    std::unique_ptr<LogicalPlan> innerPlan;
    auto info = QueryGraphPlanningInfo();
    info.predicates = predicates;
    if (correlatedExprs.empty()) {
        info.subqueryType = SubqueryType::NONE;
        innerPlan =
            planQueryGraphCollectionInNewContext(*subquery->getQueryGraphCollection(), info);
        switch (subquery->getSubqueryType()) {
        case common::SubqueryType::EXISTS: {
            appendAggregate(expression_vector{}, expression_vector{subquery->getCountStarExpr()},
                *innerPlan);
            appendProjection(expression_vector{subquery->getProjectionExpr()}, *innerPlan);
        } break;
        case common::SubqueryType::COUNT: {
            appendAggregate(expression_vector{}, expression_vector{subquery->getProjectionExpr()},
                *innerPlan);
        } break;
        default:
            KU_UNREACHABLE;
        }
        appendCrossProduct(outerPlan, *innerPlan, outerPlan);
    } else {
        info.corrExprs = correlatedExprs;
        info.corrExprsCard = outerPlan.getCardinality();
        if (isInternalIDCorrelated(*subquery->getQueryGraphCollection(), correlatedExprs)) {
            info.subqueryType = SubqueryType::INTERNAL_ID_CORRELATED;
            innerPlan =
                planQueryGraphCollectionInNewContext(*subquery->getQueryGraphCollection(), info);
        } else {
            info.subqueryType = SubqueryType::CORRELATED;
            innerPlan =
                planQueryGraphCollectionInNewContext(*subquery->getQueryGraphCollection(), info);
            appendAccumulate(correlatedExprs, outerPlan);
        }
        switch (subquery->getSubqueryType()) {
        case common::SubqueryType::EXISTS: {
            appendMarkJoin(correlatedExprs, expression, outerPlan, *innerPlan);
        } break;
        case common::SubqueryType::COUNT: {
            appendAggregate(correlatedExprs, expression_vector{subquery->getProjectionExpr()},
                *innerPlan);
            appendHashJoin(correlatedExprs, common::JoinType::COUNT, outerPlan, *innerPlan,
                outerPlan);
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
}

void Planner::planSubqueryIfNecessary(std::shared_ptr<Expression> expression, LogicalPlan& plan) {
    auto collector = SubqueryExprCollector();
    collector.visit(expression);
    if (collector.hasSubquery()) {
        for (auto& expr : collector.getSubqueryExprs()) {
            if (plan.getSchema()->isExpressionInScope(*expr)) {
                continue;
            }
            planSubquery(expr, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
