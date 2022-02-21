#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/exists/logical_exist.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/result_collector/logical_result_collector.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/planner/include/logical_plan/operator/union/logical_union.h"

namespace graphflow {
namespace planner {

vector<unique_ptr<LogicalPlan>> Enumerator::getAllPlans(const BoundRegularQuery& regularQuery) {
    if (regularQuery.getNumSingleQueries() == 1) {
        auto plans = getAllPlans(*regularQuery.getSingleQuery(0));
        for (auto& plan : plans) {
            appendResultCollector(*plan);
        }
        return plans;
    }
    vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans(
        regularQuery.getNumSingleQueries());
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        childrenLogicalPlans[i] = getAllPlans(*regularQuery.getSingleQuery(i));
    }
    auto childrenPlans = cartesianProductChildrenPlans(move(childrenLogicalPlans));
    vector<unique_ptr<LogicalPlan>> resultPlans;
    for (auto& childrenPlan : childrenPlans) {
        auto plan = createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0));
        appendResultCollector(*plan);
        resultPlans.push_back(move(plan));
    }
    return resultPlans;
}

unique_ptr<LogicalPlan> Enumerator::getBestPlan(const BoundRegularQuery& regularQuery) {
    if (regularQuery.getNumSingleQueries()) {
        auto bestPlan = getBestPlan(*regularQuery.getSingleQuery(0));
        appendResultCollector(*bestPlan);
        return bestPlan;
    }
    vector<unique_ptr<LogicalPlan>> childrenPlans;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        childrenPlans[i] = getBestPlan(*regularQuery.getSingleQuery(i));
    }
    auto bestPlan = createUnionPlan(childrenPlans, regularQuery.getIsUnionAll(0));
    appendResultCollector(*bestPlan);
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getValidSubPlans(
    vector<unique_ptr<LogicalPlan>> plans) {
    vector<unique_ptr<LogicalPlan>> results;
    for (auto& plan : plans) {
        if (plan->containOperatorsNotAllowedInSubPlan()) {
            continue;
        }
        results.push_back(move(plan));
    }
    return results;
}

unique_ptr<LogicalPlan> Enumerator::getBestPlan(vector<unique_ptr<LogicalPlan>> plans) {
    auto bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->cost < bestPlan->cost) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getAllPlans(const NormalizedSingleQuery& singleQuery) {
    vector<unique_ptr<LogicalPlan>> result;
    for (auto& plan : enumeratePlans(singleQuery)) {
        // This is copy is to avoid sharing operator across plans. Later optimization requires
        // each plan to be independent.
        plan->lastOperator = plan->lastOperator->copy();
        result.push_back(move(plan));
    }
    return result;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans(
    const NormalizedSingleQuery& singleQuery) {
    propertiesToScan = EnumeratorUtils::getPropertiesToScan(singleQuery);
    joinOrderEnumerator.resetState();
    auto plans = getInitialEmptyPlans();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        plans = enumerateQueryPart(*singleQuery.getQueryPart(i), move(plans));
    }
    for (auto& plan : plans) {
        plan->setExpressionsToCollect(singleQuery.getExpressionsToReturn());
    }
    return plans;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumerateQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    vector<unique_ptr<LogicalPlan>> plans = move(prevPlans);
    for (auto i = 0u; i < queryPart.getNumQueryGraph(); ++i) {
        if (queryPart.isQueryGraphOptional(i)) {
            auto& queryGraph = *queryPart.getQueryGraph(i);
            auto queryGraphPredicate = queryPart.getQueryGraphPredicate(i);
            for (auto& plan : plans) {
                planOptionalMatch(queryGraph, queryGraphPredicate, *plan);
            }
            // Although optional match is planned as a subquery, we still need to merge query graph
            // as we merge sub query schema into outer query.
            joinOrderEnumerator.context->mergeQueryGraph(queryGraph);
        } else {
            plans = joinOrderEnumerator.enumerateJoinOrder(
                *queryPart.getQueryGraph(i), queryPart.getQueryGraphPredicate(i), move(plans));
        }
    }
    projectionEnumerator.enumerateProjectionBody(
        *queryPart.getProjectionBody(), plans, queryPart.isLastQueryPart());
    if (queryPart.hasProjectionBodyPredicate()) {
        for (auto& plan : plans) {
            appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
        }
    }
    return plans;
}

void Enumerator::planOptionalMatch(const QueryGraph& queryGraph,
    const shared_ptr<Expression>& queryGraphPredicate, LogicalPlan& outerPlan) {
    vector<shared_ptr<Expression>> expressionsToScanFromOuter;
    if (queryGraphPredicate) {
        expressionsToScanFromOuter =
            getSubExpressionsInSchema(queryGraphPredicate, *outerPlan.schema);
    }
    for (auto& nodeIDExpression : queryGraph.getNodeIDExpressions()) {
        if (outerPlan.schema->isExpressionInScope(*nodeIDExpression)) {
            expressionsToScanFromOuter.push_back(nodeIDExpression);
        }
    }
    for (auto& expression : expressionsToScanFromOuter) {
        appendFlattenIfNecessary(
            outerPlan.schema->getGroupPos(expression->getUniqueName()), outerPlan);
    }
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToScanFromOuter));
    auto bestPlan = getBestPlan(getValidSubPlans(joinOrderEnumerator.enumerateJoinOrder(
        queryGraph, queryGraphPredicate, getInitialEmptyPlans())));
    joinOrderEnumerator.exitSubquery(move(prevContext));
    // Schema merging logic for optional match:
    //    Consider example MATCH (a:Person) OPTIONAL MATCH (a)-[:studyAt]->(b:Organisation)
    //    where (a)-[:studyAt]->(b:Organisation) is a column extend. The inner resultSet
    //    contains 1 dataChunk DCInner with 2 vectors. In the outer query, we shallow copy the
    //    vector b from the inner query. However we put b's vector not to a's datachunk DCOuterA but
    //    to a different datachunk DCOuterB. This is because the b's vector contains DCInner's
    //    state, which might be different than DCOuterA. To make sure that when vector b is copied
    //    to the outer query, we can keep DCInner's state, we copy B to a new datachunk.
    auto subPlanSchema = bestPlan->schema->copy();
    assert(subPlanSchema->getNumGroups() > 0);
    auto firstInnerGroup = subPlanSchema->getGroup(0);
    auto firstInnerGroupMapToOuterPos = UINT32_MAX;
    // Merge first inner group into outer. This merging handles the logic above.
    for (auto& expression : firstInnerGroup->getExpressions()) {
        if (outerPlan.schema->isExpressionInScope(*expression)) {
            continue;
        }
        if (firstInnerGroupMapToOuterPos == UINT32_MAX) {
            firstInnerGroupMapToOuterPos = outerPlan.schema->createGroup();
        }
        outerPlan.schema->insertToGroupAndScope(expression, firstInnerGroupMapToOuterPos);
        outerPlan.schema->getGroup(firstInnerGroupMapToOuterPos)
            ->setIsFlat(firstInnerGroup->getIsFlat());
    }
    // Merge rest inner groups into outer.
    for (auto i = 1u; i < subPlanSchema->getNumGroups(); ++i) {
        auto outerPos = outerPlan.schema->createGroup();
        auto group = subPlanSchema->getGroup(i);
        for (auto& expression : group->getExpressions()) {
            outerPlan.schema->insertToGroupAndScope(expression, outerPos);
        }
        outerPlan.schema->getGroup(outerPos)->setIsFlat(group->getIsFlat());
    }
    auto logicalLeftNestedLoopJoin = make_shared<LogicalLeftNestedLoopJoin>(
        bestPlan->schema->copy(), outerPlan.lastOperator, bestPlan->lastOperator);
    outerPlan.appendOperator(move(logicalLeftNestedLoopJoin));
}

void Enumerator::planExistsSubquery(
    const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression, LogicalPlan& outerPlan) {
    auto expressionsToScanFromOuter =
        getSubExpressionsInSchema(subqueryExpression, *outerPlan.schema);
    // We flatten all dependent groups for subquery and the result of subquery evaluation can be
    // appended to any flat dependent group.
    auto groupPosToWrite = UINT32_MAX;
    for (auto& expression : expressionsToScanFromOuter) {
        groupPosToWrite = outerPlan.schema->getGroupPos(expression->getUniqueName());
        appendFlattenIfNecessary(groupPosToWrite, outerPlan);
    }
    outerPlan.schema->insertToGroupAndScope(subqueryExpression, groupPosToWrite);
    auto& normalizedQuery = *subqueryExpression->getSubquery();
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToScanFromOuter));
    auto plans = enumerateQueryPart(*normalizedQuery.getQueryPart(0), getInitialEmptyPlans());
    joinOrderEnumerator.context->clearExpressionsToScanFromOuter();
    for (auto i = 1u; i < normalizedQuery.getNumQueryParts(); ++i) {
        plans = enumerateQueryPart(*normalizedQuery.getQueryPart(i), move(plans));
    }
    joinOrderEnumerator.exitSubquery(move(prevContext));
    auto bestPlan = getBestPlan(getValidSubPlans(move(plans)));
    auto logicalExists = make_shared<LogicalExists>(subqueryExpression, bestPlan->schema->copy(),
        outerPlan.lastOperator, bestPlan->lastOperator);
    outerPlan.appendOperator(logicalExists);
}

void Enumerator::planSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (expression->hasSubqueryExpression()) {
        for (auto& expr : expression->getTopLevelSubSubqueryExpressions()) {
            auto subqueryExpression = static_pointer_cast<ExistentialSubqueryExpression>(expr);
            planExistsSubquery(subqueryExpression, plan);
        }
    }
}

void Enumerator::appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    for (auto& groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

uint32_t Enumerator::appendFlattensButOne(
    const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    if (groupsPos.empty()) {
        // an expression may not depend on any group. E.g. COUNT(*).
        return UINT32_MAX;
    }
    vector<uint32_t> unFlatGroupsPos;
    for (auto& groupPos : groupsPos) {
        if (!plan.schema->getGroup(groupPos)->getIsFlat()) {
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

void Enumerator::appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan) {
    auto& group = *plan.schema->getGroup(groupPos);
    if (group.getIsFlat()) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += group.getEstimatedCardinality();
    auto flatten = make_shared<LogicalFlatten>(group.getAnyExpressionName(), plan.lastOperator);
    plan.appendOperator(move(flatten));
}

void Enumerator::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    planSubqueryIfNecessary(expression, plan);
    auto dependentGroupsPos = getDependentGroupsPos(expression, *plan.schema);
    auto groupPosToSelect = appendFlattensButOne(dependentGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    auto group = plan.schema->getGroup(groupPosToSelect);
    group->setEstimatedCardinality(group->getEstimatedCardinality() * PREDICATE_SELECTIVITY);
    plan.appendOperator(move(filter));
}

void Enumerator::appendScanNodeProperty(const NodeExpression& node, LogicalPlan& plan) {
    auto structuredProperties = EnumeratorUtils::getPropertiesForNode(propertiesToScan, node, true);
    if (!structuredProperties.empty()) {
        appendScanNodeProperty(structuredProperties, node, plan);
    }
    auto unstructuredProperties =
        EnumeratorUtils::getPropertiesForNode(propertiesToScan, node, false);
    if (!unstructuredProperties.empty()) {
        appendScanNodeProperty(unstructuredProperties, node, plan);
    }
}

void Enumerator::appendScanNodeProperty(
    const property_vector& properties, const NodeExpression& node, LogicalPlan& plan) {
    vector<string> propertyNames;
    vector<uint32_t> propertyKeys;
    auto groupPos = plan.schema->getGroupPos(node.getIDProperty());
    for (auto& property : properties) {
        if (plan.schema->isExpressionInScope(*property)) {
            continue;
        }
        propertyNames.push_back(property->getUniqueName());
        propertyKeys.push_back(property->getPropertyKey());
        plan.schema->insertToGroupAndScope(property, groupPos);
    }
    assert(!properties.empty());
    auto scanNodeProperty = make_shared<LogicalScanNodeProperty>(node.getIDProperty(),
        node.getLabel(), move(propertyNames), move(propertyKeys),
        properties[0]->dataType == UNSTRUCTURED, plan.lastOperator);
    plan.appendOperator(move(scanNodeProperty));
}

void Enumerator::appendScanRelProperty(const RelExpression& rel, LogicalPlan& plan) {
    for (auto& property : EnumeratorUtils::getPropertiesForRel(propertiesToScan, rel)) {
        if (plan.schema->isExpressionInScope(*property)) {
            continue;
        }
        appendScanRelProperty(property, rel, plan);
    }
}

void Enumerator::appendScanRelProperty(
    const shared_ptr<PropertyExpression>& property, const RelExpression& rel, LogicalPlan& plan) {
    auto extend = plan.schema->getExistingLogicalExtend(rel.getUniqueName());
    auto scanProperty = make_shared<LogicalScanRelProperty>(extend->boundNodeID,
        extend->boundNodeLabel, extend->nbrNodeID, extend->relLabel, extend->direction,
        property->getUniqueName(), property->getPropertyKey(), extend->isColumn, plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(extend->nbrNodeID);
    plan.schema->insertToGroupAndScope(property, groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendResultCollector(LogicalPlan& plan) {
    assert(!plan.getExpressionsToCollect().empty());
    auto resultCollector = make_shared<LogicalResultCollector>(
        plan.getExpressionsToCollect(), plan.schema->copy(), plan.lastOperator);
    plan.appendOperator(resultCollector);
}

unique_ptr<LogicalPlan> Enumerator::createUnionPlan(
    vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
    // If an expression to union has different flat/unflat state in different child, we
    // need to flatten that expression in all the single queries.
    assert(!childrenPlans.empty());
    for (auto i = 0u; i < childrenPlans[0]->getExpressionsToCollect().size(); i++) {
        bool hasFlatExpression = false;
        for (auto& childPlan : childrenPlans) {
            auto expressionName = childPlan->getExpressionsToCollect()[i]->getUniqueName();
            hasFlatExpression |= childPlan->getSchema()->getGroup(expressionName)->getIsFlat();
        }
        if (hasFlatExpression) {
            for (auto& childPlan : childrenPlans) {
                auto expressionName = childPlan->getExpressionsToCollect()[i]->getUniqueName();
                appendFlattenIfNecessary(
                    childPlan->getSchema()->getGroupPos(expressionName), *childPlan);
            }
        }
    }
    auto plan = make_unique<LogicalPlan>();
    auto logicalUnion = make_shared<LogicalUnion>(childrenPlans[0]->getExpressionsToCollect());
    for (auto& childPlan : childrenPlans) {
        appendResultCollector(*childPlan);
        Enumerator::computeSchemaForHashJoinOrderByAndUnion(
            childPlan->schema->getGroupsPosInScope(), *childPlan->schema, *plan->schema);
        plan->cost += childPlan->cost;
        logicalUnion->addChild(childPlan->lastOperator);
    }
    plan->setExpressionsToCollect(logicalUnion->getExpressionsToUnion());
    plan->appendOperator(logicalUnion);
    if (!isUnionAll) {
        projectionEnumerator.appendDistinct(logicalUnion->getExpressionsToUnion(), *plan);
    }
    return plan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getInitialEmptyPlans() {
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    return plans;
}

unordered_set<uint32_t> Enumerator::getDependentGroupsPos(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    unordered_set<uint32_t> dependentGroupsPos;
    for (auto& subExpression : getSubExpressionsInSchema(expression, schema)) {
        dependentGroupsPos.insert(schema.getGroupPos(subExpression->getUniqueName()));
    }
    return dependentGroupsPos;
}

vector<shared_ptr<Expression>> Enumerator::getSubExpressionsInSchema(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    vector<shared_ptr<Expression>> results;
    if (schema.isExpressionInScope(*expression)) {
        results.push_back(expression);
        return results;
    }

    for (auto& child : expression->getChildren()) {
        for (auto& subExpression : getSubExpressionsInSchema(child, schema)) {
            results.push_back(subExpression);
        }
    }
    return results;
}

vector<shared_ptr<Expression>> Enumerator::getSubExpressionsNotInSchemaOfType(
    const shared_ptr<Expression>& expression, const Schema& schema,
    const std::function<bool(ExpressionType)>& typeCheckFunc) {
    vector<shared_ptr<Expression>> results;
    if (schema.isExpressionInScope(*expression)) {
        return results;
    }
    if (typeCheckFunc(expression->expressionType)) {
        results.push_back(expression);
        return results;
    }
    for (auto& child : expression->getChildren()) {
        for (auto& subExpression :
            getSubExpressionsNotInSchemaOfType(child, schema, typeCheckFunc)) {
            results.push_back(subExpression);
        }
    }
    return results;
}

void Enumerator::computeSchemaForHashJoinOrderByAndUnion(
    const unordered_set<uint32_t>& groupsToMaterializePos, const Schema& schemaBeforeSink,
    Schema& schemaAfterSink) {
    auto flatVectorsOutputPos = UINT32_MAX;
    auto isAllVectorsFlat = true;
    for (auto& groupToMaterializePos : groupsToMaterializePos) {
        auto groupToMaterialize = schemaBeforeSink.getGroup(groupToMaterializePos);
        if (groupToMaterialize->getIsFlat()) {
            if (flatVectorsOutputPos == UINT32_MAX) {
                flatVectorsOutputPos = schemaAfterSink.createGroup();
            }
            schemaAfterSink.insertToGroupAndScope(
                schemaBeforeSink.getExpressionsInScope(groupToMaterializePos),
                flatVectorsOutputPos);
        } else {
            isAllVectorsFlat = false;
            auto groupPos = schemaAfterSink.createGroup();
            schemaAfterSink.insertToGroupAndScope(
                schemaBeforeSink.getExpressionsInScope(groupToMaterializePos), groupPos);
            schemaAfterSink.getGroup(groupPos)->setEstimatedCardinality(
                groupToMaterialize->getEstimatedCardinality());
        }
    }
    if (!isAllVectorsFlat && flatVectorsOutputPos != UINT32_MAX) {
        schemaAfterSink.flattenGroup(flatVectorsOutputPos);
    }
}

vector<vector<unique_ptr<LogicalPlan>>> Enumerator::cartesianProductChildrenPlans(
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
} // namespace graphflow
