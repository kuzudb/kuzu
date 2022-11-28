#include "planner/query_planner.h"

#include "binder/query/bound_regular_query.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "planner/logical_plan/logical_operator/logical_scan_rel_property.h"
#include "planner/logical_plan/logical_operator/logical_union.h"
#include "planner/logical_plan/logical_operator/logical_unwind.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

vector<unique_ptr<LogicalPlan>> QueryPlanner::getAllPlans(const BoundStatement& boundStatement) {
    vector<unique_ptr<LogicalPlan>> resultPlans;
    auto& regularQuery = (BoundRegularQuery&)boundStatement;
    if (regularQuery.getNumSingleQueries() == 1) {
        resultPlans = planSingleQuery(*regularQuery.getSingleQuery(0));
    } else {
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans(
            regularQuery.getNumSingleQueries());
        for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
            childrenLogicalPlans[i] = planSingleQuery(*regularQuery.getSingleQuery(i));
        }
        auto childrenPlans = cartesianProductChildrenPlans(move(childrenLogicalPlans));
        for (auto& childrenPlan : childrenPlans) {
            resultPlans.push_back(createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0)));
        }
    }
    for (auto& plan : resultPlans) {
        plan->setExpressionsToCollect(regularQuery.getExpressionsToReturn());
    }
    return resultPlans;
}

unique_ptr<LogicalPlan> QueryPlanner::getBestPlan(vector<unique_ptr<LogicalPlan>> plans) {
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
vector<unique_ptr<LogicalPlan>> QueryPlanner::planSingleQuery(
    const NormalizedSingleQuery& singleQuery) {
    propertiesToScan.clear();
    for (auto& expression : singleQuery.getPropertiesToRead()) {
        assert(expression->expressionType == PROPERTY);
        propertiesToScan.push_back(expression);
    }
    joinOrderEnumerator.resetState();
    auto plans = getInitialEmptyPlans();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        plans = planQueryPart(*singleQuery.getQueryPart(i), move(plans));
    }
    vector<unique_ptr<LogicalPlan>> result;
    for (auto& plan : plans) {
        // This is copy is to avoid sharing operator across plans. Later optimization requires
        // each plan to be independent.
        result.push_back(plan->deepCopy());
    }
    return result;
}

vector<unique_ptr<LogicalPlan>> QueryPlanner::planQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    vector<unique_ptr<LogicalPlan>> plans = move(prevPlans);
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
    BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& prevPlans) {
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
    BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& plans) {
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
    BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& plans) {
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

static vector<shared_ptr<NodeExpression>> getJoinNodes(expression_vector& expressions) {
    vector<shared_ptr<NodeExpression>> joinNodes;
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == NODE_ID) {
            auto node = static_pointer_cast<NodeExpression>(expression->getChild(0));
            joinNodes.push_back(std::move(node));
        }
    }
    return joinNodes;
}

void QueryPlanner::planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
    expression_vector& predicates, LogicalPlan& outerPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, outerPlan.getSchema());
    if (correlatedExpressions.empty()) {
        throw NotImplementedException("Optional match is disconnected with previous MATCH clause.");
    }
    if (ExpressionUtil::allExpressionsHaveDataType(correlatedExpressions, NODE_ID)) {
        auto joinNodes = getJoinNodes(correlatedExpressions);
        // When correlated variables are all NODE IDs, the subquery can be un-nested as left join.
        // Join nodes are scanned twice in both outer and inner. However, we make sure inner table
        // scan only scans node ID and does not scan from storage (i.e. no property scan).
        auto prevContext = joinOrderEnumerator.enterSubquery(
            &outerPlan, expression_vector{} /* nothing to scan from outer */, joinNodes);
        auto innerPlans = joinOrderEnumerator.enumerate(queryGraphCollection, predicates);
        auto bestInnerPlan = getBestPlan(std::move(innerPlans));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        for (auto& joinNode : joinNodes) {
            appendFlattenIfNecessary(joinNode->getInternalIDProperty(), outerPlan);
        }
        JoinOrderEnumerator::planLeftHashJoin(joinNodes, outerPlan, *bestInnerPlan);
    } else {
        throw NotImplementedException("Correlated optional match is not supported.");
    }
}

void QueryPlanner::planRegularMatch(const QueryGraphCollection& queryGraphCollection,
    expression_vector& predicates, LogicalPlan& prevPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraphCollection, predicates, prevPlan.getSchema());
    auto joinNodes = getJoinNodes(correlatedExpressions);
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
    auto prevContext = joinOrderEnumerator.enterSubquery(
        &prevPlan, expression_vector{} /* nothing to scan from outer */, joinNodes);
    auto plans = joinOrderEnumerator.enumerate(queryGraphCollection, predicatesToPushDown);
    joinOrderEnumerator.exitSubquery(std::move(prevContext));
    auto bestPlan = getBestPlan(std::move(plans));
    if (joinNodes.empty()) {
        JoinOrderEnumerator::planCrossProduct(prevPlan, *bestPlan);
    } else {
        JoinOrderEnumerator::planInnerHashJoin(joinNodes, prevPlan, *bestPlan);
    }
    for (auto& predicate : predicatesToPullUp) {
        appendFilter(predicate, prevPlan);
    }
}

void QueryPlanner::planExistsSubquery(shared_ptr<Expression>& expression, LogicalPlan& outerPlan) {
    assert(expression->expressionType == EXISTENTIAL_SUBQUERY);
    auto subquery = static_pointer_cast<ExistentialSubqueryExpression>(expression);
    auto correlatedExpressions = outerPlan.getSchema()->getSubExpressionsInScope(subquery);
    if (correlatedExpressions.empty()) {
        throw NotImplementedException("Subquery is disconnected with outer query.");
    }
    if (ExpressionUtil::allExpressionsHaveDataType(correlatedExpressions, NODE_ID)) {
        auto joinNodes = getJoinNodes(correlatedExpressions);
        // Unnest as mark join. See planOptionalMatch for unnesting logic.
        auto prevContext = joinOrderEnumerator.enterSubquery(
            &outerPlan, expression_vector{} /* nothing to scan from outer */, joinNodes);
        auto predicates = subquery->hasWhereExpression() ?
                              subquery->getWhereExpression()->splitOnAND() :
                              expression_vector{};
        auto bestInnerPlan = getBestPlan(
            joinOrderEnumerator.enumerate(*subquery->getQueryGraphCollection(), predicates));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        JoinOrderEnumerator::planMarkJoin(joinNodes, expression, outerPlan, *bestInnerPlan);
    } else {
        throw NotImplementedException("Correlated exists subquery is not supported.");
    }
}

void QueryPlanner::planSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (expression->hasSubqueryExpression()) {
        for (auto& expr : expression->getTopLevelSubSubqueryExpressions()) {
            planExistsSubquery(expr, plan);
        }
    }
}

void QueryPlanner::appendAccumulate(kuzu::planner::LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto schemaBeforeSink = schema->copy();
    SinkOperatorUtil::recomputeSchema(*schemaBeforeSink, *schema);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = 0u; i < schema->getNumGroups(); ++i) {
        if (schema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto sink = make_shared<LogicalAccumulate>(schemaBeforeSink->getExpressionsInScope(),
        flatOutputGroupPositions, std::move(schemaBeforeSink), plan.getLastOperator());
    plan.setLastOperator(sink);
}

void QueryPlanner::appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto groupPos = schema->createGroup();
    schema->flattenGroup(groupPos); // Mark group holding constant as flat.
    for (auto& expression : expressions) {
        // No need to insert repeated constant.
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        schema->insertToGroupAndScope(expression, groupPos);
    }
    auto expressionsScan = make_shared<LogicalExpressionsScan>(std::move(expressions));
    plan.setLastOperator(std::move(expressionsScan));
}

void QueryPlanner::appendDistinct(
    const expression_vector& expressionsToDistinct, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    for (auto& expression : expressionsToDistinct) {
        auto dependentGroupsPos = schema->getDependentGroupsPos(expression);
        appendFlattens(dependentGroupsPos, plan);
    }
    auto distinct =
        make_shared<LogicalDistinct>(expressionsToDistinct, schema->copy(), plan.getLastOperator());
    schema->clear();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    plan.setLastOperator(move(distinct));
}

void QueryPlanner::appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto dependentGroupPos = schema->getDependentGroupsPos(boundUnwindClause.getExpression());
    if (!dependentGroupPos.empty()) {
        appendFlattens(dependentGroupPos, plan);
    }
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(boundUnwindClause.getAliasExpression(), groupPos);
    auto logicalUnwind = make_shared<LogicalUnwind>(boundUnwindClause.getExpression(),
        boundUnwindClause.getAliasExpression(), plan.getLastOperator());
    plan.setLastOperator(logicalUnwind);
}

void QueryPlanner::appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    for (auto& groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

uint32_t QueryPlanner::appendFlattensButOne(
    const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    if (groupsPos.empty()) {
        // an expression may not depend on any group. E.g. COUNT(*).
        return UINT32_MAX;
    }
    vector<uint32_t> unFlatGroupsPos;
    for (auto& groupPos : groupsPos) {
        if (!plan.getSchema()->getGroup(groupPos)->getIsFlat()) {
            unFlatGroupsPos.push_back(groupPos);
        }
    }
    if (unFlatGroupsPos.empty()) {
        return *groupsPos.begin();
    }
    for (auto i = 1u; i < unFlatGroupsPos.size(); ++i) {
        appendFlattenIfNecessary(unFlatGroupsPos[i], plan);
    }
    return unFlatGroupsPos[0];
}

void QueryPlanner::appendFlattenIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto group = schema->getGroup(expression);
    if (group->getIsFlat()) {
        return;
    }
    auto flatten = make_shared<LogicalFlatten>(expression, plan.getLastOperator());
    flatten->computeSchema(*schema);
    plan.setLastOperator(std::move(flatten));
    // update cardinality estimation info
    plan.multiplyCardinality(group->getMultiplier());
}

void QueryPlanner::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    planSubqueryIfNecessary(expression, plan);
    auto dependentGroupsPos = plan.getSchema()->getDependentGroupsPos(expression);
    auto groupPosToSelect = appendFlattensButOne(dependentGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.getLastOperator());
    plan.multiplyCardinality(EnumeratorKnobs::PREDICATE_SELECTIVITY);
    plan.setLastOperator(std::move(filter));
}

void QueryPlanner::appendScanNodePropIfNecessary(const expression_vector& propertyExpressions,
    shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    expression_vector propertyExpressionToScan;
    auto groupPos = schema->getGroupPos(node->getInternalIDPropertyName());
    for (auto& propertyExpression : propertyExpressions) {
        if (schema->isExpressionInScope(*propertyExpression)) {
            continue;
        }
        propertyExpressionToScan.push_back(propertyExpression);
        schema->insertToGroupAndScope(propertyExpression, groupPos);
    }
    if (propertyExpressionToScan.empty()) { // all properties have been scanned before
        return;
    }
    auto scanNodeProperty = make_shared<LogicalScanNodeProperty>(
        std::move(node), std::move(propertyExpressionToScan), plan.getLastOperator());
    plan.setLastOperator(std::move(scanNodeProperty));
}

void QueryPlanner::appendScanRelPropIfNecessary(shared_ptr<Expression>& expression,
    RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    if (schema->isExpressionInScope(*expression)) {
        return;
    }
    assert(!rel.isVariableLength());
    auto boundNode = FWD == direction ? rel.getSrcNode() : rel.getDstNode();
    auto nbrNode = FWD == direction ? rel.getDstNode() : rel.getSrcNode();
    auto isColumn =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel.getTableID(), direction);
    assert(expression->expressionType == PROPERTY);
    auto property = static_pointer_cast<PropertyExpression>(expression);
    auto relTableID = rel.getTableID();
    auto scanProperty = make_shared<LogicalScanRelProperty>(boundNode, nbrNode, relTableID,
        direction, property->getUniqueName(), property->getPropertyID(relTableID), isColumn,
        plan.getLastOperator());
    auto groupPos = schema->getGroupPos(nbrNode->getInternalIDPropertyName());
    schema->insertToGroupAndScope(property, groupPos);
    plan.setLastOperator(move(scanProperty));
}

unique_ptr<LogicalPlan> QueryPlanner::createUnionPlan(
    vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
    // If an expression to union has different flat/unflat state in different child, we
    // need to flatten that expression in all the single queries.
    assert(!childrenPlans.empty());
    auto firstChildSchema = childrenPlans[0]->getSchema();
    auto numExpressionsToUnion = firstChildSchema->getExpressionsInScope().size();
    for (auto i = 0u; i < numExpressionsToUnion; i++) {
        bool hasFlatExpression = false;
        for (auto& childPlan : childrenPlans) {
            auto childSchema = childPlan->getSchema();
            auto expressionName = childSchema->getExpressionsInScope()[i]->getUniqueName();
            hasFlatExpression |= childSchema->getGroup(expressionName)->getIsFlat();
        }
        if (hasFlatExpression) {
            for (auto& childPlan : childrenPlans) {
                auto childSchema = childPlan->getSchema();
                auto expressionName = childSchema->getExpressionsInScope()[i]->getUniqueName();
                appendFlattenIfNecessary(childSchema->getGroupPos(expressionName), *childPlan);
            }
        }
    }
    // we compute the schema based on first child
    auto plan = make_unique<LogicalPlan>();
    SinkOperatorUtil::recomputeSchema(*firstChildSchema, *plan->getSchema());
    vector<unique_ptr<Schema>> schemaBeforeUnion;
    vector<shared_ptr<LogicalOperator>> children;
    for (auto& childPlan : childrenPlans) {
        plan->increaseCost(childPlan->getCost());
        schemaBeforeUnion.push_back(childPlan->getSchema()->copy());
        children.push_back(childPlan->getLastOperator());
    }
    auto logicalUnion = make_shared<LogicalUnion>(
        firstChildSchema->getExpressionsInScope(), move(schemaBeforeUnion), move(children));
    plan->setLastOperator(logicalUnion);
    if (!isUnionAll) {
        appendDistinct(logicalUnion->getExpressionsToUnion(), *plan);
    }
    return plan;
}

vector<unique_ptr<LogicalPlan>> QueryPlanner::getInitialEmptyPlans() {
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    return plans;
}

expression_vector QueryPlanner::getPropertiesForNode(NodeExpression& node) {
    expression_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == node.getUniqueName()) {
            assert(property->getChild(0)->dataType.typeID == NODE);
            result.push_back(property);
        }
    }
    return result;
}

expression_vector QueryPlanner::getPropertiesForRel(RelExpression& rel) {
    expression_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == rel.getUniqueName()) {
            assert(property->getChild(0)->dataType.typeID == REL);
            result.push_back(property);
        }
    }
    return result;
}

vector<vector<unique_ptr<LogicalPlan>>> QueryPlanner::cartesianProductChildrenPlans(
    vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans) {
    vector<vector<unique_ptr<LogicalPlan>>> resultChildrenPlans;
    for (auto& childLogicalPlans : childrenLogicalPlans) {
        vector<vector<unique_ptr<LogicalPlan>>> curChildResultLogicalPlans;
        for (auto& childLogicalPlan : childLogicalPlans) {
            if (resultChildrenPlans.empty()) {
                vector<unique_ptr<LogicalPlan>> logicalPlans;
                logicalPlans.push_back(childLogicalPlan->deepCopy());
                curChildResultLogicalPlans.push_back(move(logicalPlans));
            } else {
                for (auto& resultChildPlans : resultChildrenPlans) {
                    vector<unique_ptr<LogicalPlan>> logicalPlans;
                    for (auto& resultChildPlan : resultChildPlans) {
                        logicalPlans.push_back(resultChildPlan->deepCopy());
                    }
                    logicalPlans.push_back(childLogicalPlan->deepCopy());
                    curChildResultLogicalPlans.push_back(move(logicalPlans));
                }
            }
        }
        resultChildrenPlans = move(curChildResultLogicalPlans);
    }
    return resultChildrenPlans;
}

} // namespace planner
} // namespace kuzu
