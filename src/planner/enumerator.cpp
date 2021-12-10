#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/exists/logical_exist.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Enumerator::getBestPlan(vector<unique_ptr<LogicalPlan>> plans) {
    auto bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->cost < bestPlan->cost) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans(const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = QueryNormalizer::normalizeQuery(singleQuery);
    auto plans = getInitialEmptyPlans();
    for (auto& queryPart : normalizedQuery->getQueryParts()) {
        plans = enumerateQueryPart(*queryPart, move(plans));
    }
    return plans;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumerateQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    auto plans = joinOrderEnumerator.enumerateJoinOrder(queryPart, move(prevPlans));
    projectionEnumerator.enumerateProjectionBody(
        *queryPart.getProjectionBody(), plans, queryPart.isLastQueryPart());
    return plans;
}

void Enumerator::planExistsSubquery(
    const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression, LogicalPlan& outerPlan) {
    vector<shared_ptr<Expression>> expressionsToSelect;
    auto subExpressions = getSubExpressionsInSchema(subqueryExpression, *outerPlan.schema);
    // We flatten all dependent groups for subquery and the result of subquery evaluation can be
    // appended to any flat dependent group.
    auto groupPosToWrite = UINT32_MAX;
    for (auto& subExpression : subExpressions) {
        expressionsToSelect.push_back(subExpression);
        groupPosToWrite = outerPlan.schema->getGroupPos(subExpression->getUniqueName());
        appendFlattenIfNecessary(groupPosToWrite, outerPlan);
    }
    outerPlan.schema->getGroup(groupPosToWrite)
        ->insertExpression(subqueryExpression->getUniqueName());
    auto& normalizedQuery = *subqueryExpression->getNormalizedSubquery();
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToSelect));
    auto plans = enumerateQueryPart(*normalizedQuery.getQueryPart(0), getInitialEmptyPlans());
    joinOrderEnumerator.context->clearExpressionsToSelectFromOuter();
    for (auto i = 1u; i < normalizedQuery.getQueryParts().size(); ++i) {
        plans = enumerateQueryPart(*normalizedQuery.getQueryPart(i), move(plans));
    }
    joinOrderEnumerator.exitSubquery(move(prevContext));
    auto logicalExists = make_shared<LogicalExists>(
        subqueryExpression, getBestPlan(move(plans)), outerPlan.lastOperator);
    outerPlan.appendOperator(logicalExists);
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
        if (!plan.schema->getGroup(groupPos)->isFlat) {
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
    auto& group = *plan.schema->groups[groupPos];
    if (group.isFlat) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += group.estimatedCardinality;
    auto flatten = make_shared<LogicalFlatten>(group.getAnyExpressionName(), plan.lastOperator);
    plan.appendOperator(move(flatten));
}

void Enumerator::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
    auto dependentGroupsPos = getDependentGroupsPos(expression, *plan.schema);
    auto groupPosToSelect = appendFlattensButOne(dependentGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    plan.schema->groups[groupPosToSelect]->estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(filter));
}

void Enumerator::appendScanPropertiesAndPlanSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    appendScanPropertiesIfNecessary(expression, plan);
    if (expression->hasSubqueryExpression()) {
        auto expressions = expression->getTopLevelSubSubqueryExpressions();
        for (auto& expr : expressions) {
            auto subqueryExpression = static_pointer_cast<ExistentialSubqueryExpression>(expr);
            planExistsSubquery(subqueryExpression, plan);
        }
    }
}

void Enumerator::appendScanPropertiesIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    for (auto& expr : getPropertyExpressionsNotInSchema(expression, *plan.schema)) {
        auto& propertyExpression = (PropertyExpression&)*expr;
        // skip properties that has been evaluated
        if (plan.schema->containExpression(propertyExpression.getUniqueName())) {
            continue;
        }
        NODE == propertyExpression.children[0]->dataType ?
            appendScanNodePropertyIfNecessary(propertyExpression, plan) :
            appendScanRelPropertyIfNecessary(propertyExpression, plan);
    }
}

void Enumerator::appendScanNodePropertyIfNecessary(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& nodeExpression = (const NodeExpression&)*propertyExpression.children[0];
    if (!plan.schema->containExpression(nodeExpression.getIDProperty())) {
        return;
    }
    auto scanProperty = make_shared<LogicalScanNodeProperty>(nodeExpression.getIDProperty(),
        nodeExpression.label, propertyExpression.getUniqueName(), propertyExpression.propertyKey,
        UNSTRUCTURED == propertyExpression.dataType, plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(nodeExpression.getIDProperty());
    plan.schema->insertToGroup(propertyExpression.getUniqueName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendScanRelPropertyIfNecessary(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& relExpression = (const RelExpression&)*propertyExpression.children[0];
    if (!plan.schema->containLogicalExtend(relExpression.getUniqueName())) {
        return;
    }
    auto extend = plan.schema->getExistingLogicalExtend(relExpression.getUniqueName());
    auto scanProperty = make_shared<LogicalScanRelProperty>(extend->boundNodeID,
        extend->boundNodeLabel, extend->nbrNodeID, extend->relLabel, extend->direction,
        propertyExpression.getUniqueName(), propertyExpression.propertyKey, extend->isColumn,
        plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(extend->nbrNodeID);
    plan.schema->insertToGroup(propertyExpression.getUniqueName(), groupPos);
    plan.appendOperator(move(scanProperty));
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
    if (schema.containExpression(expression->getUniqueName())) {
        results.push_back(expression);
        return results;
    }
    if (EXISTENTIAL_SUBQUERY == expression->expressionType) {
        auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
        for (auto& child : subqueryExpression.getSubExpressions()) {
            for (auto& subExpression : getSubExpressionsInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    } else {
        for (auto& child : expression->children) {
            for (auto& subExpression : getSubExpressionsInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    }
    return results;
}

vector<shared_ptr<Expression>> Enumerator::getSubExpressionsNotInSchemaOfType(
    const shared_ptr<Expression>& expression, const Schema& schema,
    const std::function<bool(ExpressionType)>& typeCheckFunc) {
    vector<shared_ptr<Expression>> results;
    if (schema.containExpression(expression->getUniqueName())) {
        return results;
    }
    if (typeCheckFunc(expression->expressionType)) {
        results.push_back(expression);
        return results;
    }
    if (EXISTENTIAL_SUBQUERY == expression->expressionType) {
        auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
        for (auto& child : subqueryExpression.getSubExpressions()) {
            for (auto& subExpression :
                getSubExpressionsNotInSchemaOfType(child, schema, typeCheckFunc)) {
                results.push_back(subExpression);
            }
        }
    } else {
        for (auto& child : expression->children) {
            for (auto& subExpression :
                getSubExpressionsNotInSchemaOfType(child, schema, typeCheckFunc)) {
                results.push_back(subExpression);
            }
        }
    }
    return results;
}

} // namespace planner
} // namespace graphflow
