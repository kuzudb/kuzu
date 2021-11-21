#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
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
    if (queryPart.hasLoadCSVStatement()) {
        assert(prevPlans.size() == 1); // load csv must be the first clause
        appendLoadCSV(*queryPart.getLoadCSVStatement(), *prevPlans[0]);
    }
    auto plans = joinOrderEnumerator.enumerateJoinOrder(queryPart, move(prevPlans));
    projectionEnumerator.enumerateProjectionBody(
        *queryPart.getProjectionBody(), plans, queryPart.isLastQueryPart());
    return plans;
}

void Enumerator::planSubquery(
    const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression, LogicalPlan& outerPlan) {
    vector<shared_ptr<Expression>> expressionsToSelect;
    auto expressions = getExpressionsInSchema(subqueryExpression, *outerPlan.schema);
    for (auto& expression : expressions) {
        expressionsToSelect.push_back(expression);
        appendFlattenIfNecessary(
            outerPlan.schema->getGroupPos(expression->getUniqueName()), outerPlan);
    }
    auto& normalizedQuery = *subqueryExpression->getNormalizedSubquery();
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToSelect));
    auto plans = enumerateQueryPart(*normalizedQuery.getQueryPart(0), getInitialEmptyPlans());
    joinOrderEnumerator.context->clearExpressionsToSelectFromOuter();
    for (auto i = 1u; i < normalizedQuery.getQueryParts().size(); ++i) {
        plans = enumerateQueryPart(*normalizedQuery.getQueryPart(i), move(plans));
    }
    joinOrderEnumerator.exitSubquery(move(prevContext));
    subqueryExpression->setSubPlan(getBestPlan(move(plans)));
}

void Enumerator::appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan) {
    auto loadCSV = make_shared<LogicalLoadCSV>(loadCSVStatement.filePath,
        loadCSVStatement.tokenSeparator, loadCSVStatement.csvColumnVariables);
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : loadCSVStatement.csvColumnVariables) {
        plan.schema->insertToGroup(expression->getUniqueName(), groupPos);
    }
    plan.appendOperator(move(loadCSV));
}

void Enumerator::appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    for (auto& groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

uint32_t Enumerator::appendFlattensButOne(
    const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
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
    uint32_t groupPosToSelect =
        appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(expression, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    plan.schema->groups[groupPosToSelect]->estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(filter));
}

uint32_t Enumerator::appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    appendScanPropertiesIfNecessary(expression, plan);
    if (expression->hasSubqueryExpression()) {
        auto expressions = expression->getDependentSubqueryExpressions();
        for (auto& expr : expressions) {
            auto subqueryExpression = static_pointer_cast<ExistentialSubqueryExpression>(expr);
            if (!subqueryExpression->hasSubPlan()) {
                planSubquery(subqueryExpression, plan);
            }
        }
    }
    unordered_set<uint32_t> groupsPos;
    for (auto& expr : getExpressionsInSchema(expression, *plan.schema)) {
        groupsPos.insert(plan.schema->getGroupPos(expr->getUniqueName()));
    }
    return appendFlattensButOne(groupsPos, plan);
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

vector<shared_ptr<Expression>> Enumerator::getExpressionsInSchema(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    vector<shared_ptr<Expression>> results;
    if (schema.containExpression(expression->getUniqueName())) {
        results.push_back(expression);
        return results;
    }
    if (EXISTENTIAL_SUBQUERY == expression->expressionType) {
        auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
        for (auto& child : subqueryExpression.getDependentExpressions()) {
            for (auto& subExpression : getExpressionsInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    } else {
        for (auto& child : expression->children) {
            for (auto& subExpression : getExpressionsInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    }
    return results;
}

vector<shared_ptr<Expression>> Enumerator::getPropertyExpressionsNotInSchema(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    vector<shared_ptr<Expression>> results;
    if (schema.containExpression(expression->getUniqueName())) {
        return results;
    }
    if (PROPERTY == expression->expressionType) {
        results.push_back(expression);
        return results;
    }
    if (EXISTENTIAL_SUBQUERY == expression->expressionType) {
        auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
        for (auto& child : subqueryExpression.getDependentExpressions()) {
            for (auto& subExpression : getPropertyExpressionsNotInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    } else {
        for (auto& child : expression->children) {
            for (auto& subExpression : getPropertyExpressionsNotInSchema(child, schema)) {
                results.push_back(subExpression);
            }
        }
    }
    return results;
}

} // namespace planner
} // namespace graphflow
