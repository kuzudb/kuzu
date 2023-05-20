#include "planner/query_planner.h"

#include "binder/query/bound_regular_query.h"
#include "binder/visitor/property_collector.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "planner/logical_plan/logical_operator/logical_union.h"
#include "planner/logical_plan/logical_operator/logical_unwind.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::getAllPlans(
    const BoundStatement& boundStatement) {
    std::vector<std::unique_ptr<LogicalPlan>> resultPlans;
    auto& regularQuery = (BoundRegularQuery&)boundStatement;
    if (regularQuery.getNumSingleQueries() == 1) {
        resultPlans = planSingleQuery(*regularQuery.getSingleQuery(0));
    } else {
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans(
            regularQuery.getNumSingleQueries());
        for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
            childrenLogicalPlans[i] = planSingleQuery(*regularQuery.getSingleQuery(i));
        }
        auto childrenPlans = cartesianProductChildrenPlans(std::move(childrenLogicalPlans));
        for (auto& childrenPlan : childrenPlans) {
            resultPlans.push_back(createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0)));
        }
    }
    return resultPlans;
}

std::unique_ptr<LogicalPlan> QueryPlanner::getBestPlan(
    std::vector<std::unique_ptr<LogicalPlan>> plans) {
    auto bestPlan = std::move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->getCost() < bestPlan->getCost()) {
            bestPlan = std::move(plans[i]);
        }
    }
    return bestPlan;
}

// Note: we cannot append ResultCollector for plans enumerated for single query before there could
// be a UNION on top which requires further flatten. So we delay ResultCollector appending to
// enumerate regular query level.
std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::planSingleQuery(
    const NormalizedSingleQuery& singleQuery) {
    auto propertyCollector = binder::PropertyCollector();
    propertyCollector.visitSingleQuery(singleQuery);
    propertiesToScan = propertyCollector.getProperties();
    joinOrderEnumerator.resetState();
    auto plans = getInitialEmptyPlans();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        plans = planQueryPart(*singleQuery.getQueryPart(i), std::move(plans));
    }
    return plans;
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::planQueryPart(
    const NormalizedQueryPart& queryPart, std::vector<std::unique_ptr<LogicalPlan>> prevPlans) {
    std::vector<std::unique_ptr<LogicalPlan>> plans = std::move(prevPlans);
    // plan read
    for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
        planReadingClause(queryPart.getReadingClause(i), plans);
    }
    // plan update
    for (auto i = 0u; i < queryPart.getNumUpdatingClause(); ++i) {
        updatePlanner.planUpdatingClause(*queryPart.getUpdatingClause(i), plans);
    }
    if (queryPart.hasProjectionBody()) {
        projectionPlanner.planProjectionBody(*queryPart.getProjectionBody(), plans);
        if (queryPart.hasProjectionBodyPredicate()) {
            for (auto& plan : plans) {
                appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
            }
        }
    }
    return plans;
}

void QueryPlanner::planReadingClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    auto readingClauseType = boundReadingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        planMatchClause(boundReadingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(boundReadingClause, prevPlans);
    } break;
    default:
        assert(false);
    }
}

void QueryPlanner::planMatchClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundMatchClause = (BoundMatchClause*)boundReadingClause;
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    auto predicates = boundMatchClause->hasWhereExpression() ?
                          boundMatchClause->getWhereExpression()->splitOnAND() :
                          expression_vector{};
    // TODO(Xiyang): when we plan a set of LogicalPlans with a (OPTIONAL)MATCH we shouldn't only
    // take the best plan from (OPTIONAL)MATCH instead we should take all plans and do cartesian
    // product with the set of LogicalPlans.
    if (boundMatchClause->getIsOptional()) {
        for (auto& plan : plans) {
            planOptionalMatch(*queryGraphCollection, predicates, *plan);
        }
    } else {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            plans = joinOrderEnumerator.enumerate(*queryGraphCollection, predicates);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan);
            }
        }
    }
}

void QueryPlanner::planUnwindClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundUnwindClause = (BoundUnwindClause*)boundReadingClause;
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            auto expressions = expression_vector{boundUnwindClause->getExpression()};
            appendExpressionsScan(expressions, *plan);
        }
        appendUnwind(*boundUnwindClause, *plan);
    }
}

static expression_vector getCorrelatedExpressions(
    const QueryGraphCollection& collection, expression_vector& predicates, Schema* outerSchema) {
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
    expression_vector& predicates, LogicalPlan& outerPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, outerPlan.getSchema());
    if (correlatedExpressions.empty()) {
        throw NotImplementedException("Optional match is disconnected with previous MATCH clause.");
    }
    if (ExpressionUtil::allExpressionsHaveDataType(
            correlatedExpressions, LogicalTypeID::INTERNAL_ID)) {
        auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
        // When correlated variables are all NODE IDs, the subquery can be un-nested as left join.
        // Join nodes are scanned twice in both outer and inner. However, we make sure inner table
        // scan only scans node ID and does not scan from storage (i.e. no property scan).
        auto prevContext = joinOrderEnumerator.enterSubquery(joinNodeIDs);
        auto innerPlans = joinOrderEnumerator.enumerate(queryGraphCollection, predicates);
        auto bestInnerPlan = getBestPlan(std::move(innerPlans));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        joinOrderEnumerator.planLeftHashJoin(joinNodeIDs, outerPlan, *bestInnerPlan);
    } else {
        throw NotImplementedException("Correlated optional match is not supported.");
    }
}

void QueryPlanner::planRegularMatch(const QueryGraphCollection& queryGraphCollection,
    expression_vector& predicates, LogicalPlan& prevPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, prevPlan.getSchema());
    auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
    expression_vector predicatesToPushDown, predicatesToPullUp;
    // E.g. MATCH (a) WITH COUNT(*) AS s MATCH (b) WHERE b.age > s
    // "b.age > s" should be pulled up after both MATCH clauses are joined.
    for (auto& predicate : predicates) {
        if (prevPlan.getSchema()->getSubExpressionsInScope(predicate).empty()) {
            predicatesToPushDown.push_back(predicate);
        } else {
            predicatesToPullUp.push_back(predicate);
        }
    }
    // Multi-part query is actually CTE and CTE can be considered as a subquery but does not scan
    // from outer (i.e. can always be un-nest). So we plan multi-part query in the same way as
    // planning an un-nest subquery.
    auto prevContext = joinOrderEnumerator.enterSubquery(joinNodeIDs);
    auto plans = joinOrderEnumerator.enumerate(queryGraphCollection, predicatesToPushDown);
    joinOrderEnumerator.exitSubquery(std::move(prevContext));
    auto bestPlan = getBestPlan(std::move(plans));
    if (joinNodeIDs.empty()) {
        joinOrderEnumerator.planCrossProduct(prevPlan, *bestPlan);
    } else {
        joinOrderEnumerator.planInnerHashJoin(joinNodeIDs, prevPlan, *bestPlan);
    }
    for (auto& predicate : predicatesToPullUp) {
        appendFilter(predicate, prevPlan);
    }
}

void QueryPlanner::planExistsSubquery(
    std::shared_ptr<Expression>& expression, LogicalPlan& outerPlan) {
    assert(expression->expressionType == EXISTENTIAL_SUBQUERY);
    auto subquery = static_pointer_cast<ExistentialSubqueryExpression>(expression);
    auto correlatedExpressions = outerPlan.getSchema()->getSubExpressionsInScope(subquery);
    if (correlatedExpressions.empty()) {
        throw NotImplementedException("Subquery is disconnected with outer query.");
    }
    if (ExpressionUtil::allExpressionsHaveDataType(
            correlatedExpressions, LogicalTypeID::INTERNAL_ID)) {
        auto joinNodeIDs = getJoinNodeIDs(correlatedExpressions);
        // Unnest as mark join. See planOptionalMatch for unnesting logic.
        auto prevContext = joinOrderEnumerator.enterSubquery(joinNodeIDs);
        auto predicates = subquery->hasWhereExpression() ?
                              subquery->getWhereExpression()->splitOnAND() :
                              expression_vector{};
        auto bestInnerPlan = getBestPlan(
            joinOrderEnumerator.enumerate(*subquery->getQueryGraphCollection(), predicates));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        joinOrderEnumerator.planMarkJoin(joinNodeIDs, expression, outerPlan, *bestInnerPlan);
    } else {
        throw NotImplementedException("Correlated exists subquery is not supported.");
    }
}

void QueryPlanner::planSubqueryIfNecessary(
    const std::shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (expression->hasSubqueryExpression()) {
        for (auto& expr : expression->getTopLevelSubSubqueryExpressions()) {
            planExistsSubquery(expr, plan);
        }
    }
}

void QueryPlanner::appendAccumulate(LogicalPlan& plan) {
    auto sink = make_shared<LogicalAccumulate>(plan.getLastOperator());
    sink->computeFactorizedSchema();
    plan.setLastOperator(sink);
}

void QueryPlanner::appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto expressionsScan = make_shared<LogicalExpressionsScan>(expressions);
    expressionsScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(expressionsScan));
}

void QueryPlanner::appendDistinct(
    const expression_vector& expressionsToDistinct, LogicalPlan& plan) {
    auto distinct = make_shared<LogicalDistinct>(expressionsToDistinct, plan.getLastOperator());
    QueryPlanner::appendFlattens(distinct->getGroupsPosToFlatten(), plan);
    distinct->setChild(0, plan.getLastOperator());
    distinct->computeFactorizedSchema();
    plan.setLastOperator(std::move(distinct));
}

void QueryPlanner::appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    auto unwind = make_shared<LogicalUnwind>(boundUnwindClause.getExpression(),
        boundUnwindClause.getAliasExpression(), plan.getLastOperator());
    QueryPlanner::appendFlattens(unwind->getGroupsPosToFlatten(), plan);
    unwind->setChild(0, plan.getLastOperator());
    unwind->computeFactorizedSchema();
    plan.setLastOperator(unwind);
}

void QueryPlanner::appendFlattens(const f_group_pos_set& groupsPos, LogicalPlan& plan) {
    for (auto groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

void QueryPlanner::appendFlattenIfNecessary(f_group_pos groupPos, LogicalPlan& plan) {
    auto group = plan.getSchema()->getGroup(groupPos);
    if (group->isFlat()) {
        return;
    }
    auto flatten = make_shared<LogicalFlatten>(groupPos, plan.getLastOperator());
    flatten->computeFactorizedSchema();
    // update cardinality
    plan.setCardinality(cardinalityEstimator->estimateFlatten(plan, groupPos));
    plan.setLastOperator(std::move(flatten));
}

void QueryPlanner::appendFilters(
    const binder::expression_vector& predicates, kuzu::planner::LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        appendFilter(predicate, plan);
    }
}

void QueryPlanner::appendFilter(const std::shared_ptr<Expression>& predicate, LogicalPlan& plan) {
    planSubqueryIfNecessary(predicate, plan);
    auto filter = make_shared<LogicalFilter>(predicate, plan.getLastOperator());
    QueryPlanner::appendFlattens(filter->getGroupsPosToFlatten(), plan);
    filter->setChild(0, plan.getLastOperator());
    filter->computeFactorizedSchema();
    // estimate cardinality
    plan.setCardinality(cardinalityEstimator->estimateFilter(plan, *predicate));
    plan.setLastOperator(std::move(filter));
}

void QueryPlanner::appendScanNodePropIfNecessary(const expression_vector& propertyExpressions,
    std::shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    expression_vector propertyExpressionToScan;
    for (auto& propertyExpression : propertyExpressions) {
        if (plan.getSchema()->isExpressionInScope(*propertyExpression)) {
            continue;
        }
        propertyExpressionToScan.push_back(propertyExpression);
    }
    if (propertyExpressionToScan.empty()) { // all properties have been scanned before
        return;
    }
    auto scanNodeProperty = make_shared<LogicalScanNodeProperty>(
        std::move(node), std::move(propertyExpressionToScan), plan.getLastOperator());
    scanNodeProperty->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanNodeProperty));
}

std::unique_ptr<LogicalPlan> QueryPlanner::createUnionPlan(
    std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
    assert(!childrenPlans.empty());
    auto plan = std::make_unique<LogicalPlan>();
    std::vector<std::shared_ptr<LogicalOperator>> children;
    for (auto& childPlan : childrenPlans) {
        children.push_back(childPlan->getLastOperator());
    }
    // we compute the schema based on first child
    auto union_ = make_shared<LogicalUnion>(
        childrenPlans[0]->getSchema()->getExpressionsInScope(), std::move(children));
    for (auto i = 0u; i < childrenPlans.size(); ++i) {
        appendFlattens(union_->getGroupsPosToFlatten(i), *childrenPlans[i]);
        union_->setChild(i, childrenPlans[i]->getLastOperator());
    }
    union_->computeFactorizedSchema();
    plan->setLastOperator(union_);
    if (!isUnionAll) {
        appendDistinct(union_->getExpressionsToUnion(), *plan);
    }
    return plan;
}

std::vector<std::unique_ptr<LogicalPlan>> QueryPlanner::getInitialEmptyPlans() {
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    plans.push_back(std::make_unique<LogicalPlan>());
    return plans;
}

expression_vector QueryPlanner::getPropertiesForNode(NodeExpression& node) {
    expression_vector result;
    for (auto& expression : propertiesToScan) {
        auto property = (PropertyExpression*)expression.get();
        if (property->getVariableName() == node.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector QueryPlanner::getPropertiesForRel(RelExpression& rel) {
    expression_vector result;
    for (auto& expression : propertiesToScan) {
        auto property = (PropertyExpression*)expression.get();
        if (property->getVariableName() == rel.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

std::vector<std::vector<std::unique_ptr<LogicalPlan>>> QueryPlanner::cartesianProductChildrenPlans(
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans) {
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> resultChildrenPlans;
    for (auto& childLogicalPlans : childrenLogicalPlans) {
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> curChildResultLogicalPlans;
        for (auto& childLogicalPlan : childLogicalPlans) {
            if (resultChildrenPlans.empty()) {
                std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
                logicalPlans.push_back(childLogicalPlan->shallowCopy());
                curChildResultLogicalPlans.push_back(std::move(logicalPlans));
            } else {
                for (auto& resultChildPlans : resultChildrenPlans) {
                    std::vector<std::unique_ptr<LogicalPlan>> logicalPlans;
                    for (auto& resultChildPlan : resultChildPlans) {
                        logicalPlans.push_back(resultChildPlan->shallowCopy());
                    }
                    logicalPlans.push_back(childLogicalPlan->shallowCopy());
                    curChildResultLogicalPlans.push_back(std::move(logicalPlans));
                }
            }
        }
        resultChildrenPlans = std::move(curChildResultLogicalPlans);
    }
    return resultChildrenPlans;
}

} // namespace planner
} // namespace kuzu
