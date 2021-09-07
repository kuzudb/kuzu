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
    ExistentialSubqueryExpression& subqueryExpression, LogicalPlan& outerPlan) {
    vector<shared_ptr<Expression>> expressionsToSelect;
    for (auto& expression : subqueryExpression.getDependentLeafExpressions()) {
        if (!outerPlan.schema->containVariable(expression->getInternalName())) {
            continue;
        }
        expressionsToSelect.push_back(expression);
        appendFlattenIfNecessary(
            outerPlan.schema->getGroupPos(expression->getInternalName()), outerPlan);
    }
    auto& normalizedQuery = *subqueryExpression.getNormalizedSubquery();
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToSelect));
    auto plans = enumerateQueryPart(*normalizedQuery.getQueryPart(0), getInitialEmptyPlans());
    joinOrderEnumerator.context->clearExpressionsToSelectFromOuter();
    for (auto i = 1u; i < normalizedQuery.getQueryParts().size(); ++i) {
        plans = enumerateQueryPart(*normalizedQuery.getQueryPart(i), move(plans));
    }
    joinOrderEnumerator.exitSubquery(move(prevContext));
    subqueryExpression.setSubPlan(getBestPlan(move(plans)));
}

void Enumerator::appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan) {
    auto loadCSV = make_shared<LogicalLoadCSV>(loadCSVStatement.filePath,
        loadCSVStatement.tokenSeparator, loadCSVStatement.csvColumnVariables);
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : loadCSVStatement.csvColumnVariables) {
        plan.schema->appendToGroup(expression->getInternalName(), groupPos);
    }
    plan.appendOperator(move(loadCSV));
}

uint32_t Enumerator::appendFlattensButOne(
    const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan) {
    // randomly pick the first unFlat group to select and flatten the rest
    auto finalUnFlatGroupPos = *unFlatGroupsPos.begin();
    for (auto& unFlatGroupPos : unFlatGroupsPos) {
        if (unFlatGroupPos != finalUnFlatGroupPos) {
            appendFlattenIfNecessary(unFlatGroupPos, plan);
        }
    }
    return finalUnFlatGroupPos;
}

void Enumerator::appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan) {
    auto& group = *plan.schema->groups[groupPos];
    if (group.isFlat) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += group.estimatedCardinality;
    auto flatten = make_shared<LogicalFlatten>(group.getAnyVariable(), plan.lastOperator);
    plan.appendOperator(move(flatten));
}

void Enumerator::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    uint32_t groupPosToSelect =
        appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(*expression, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    plan.schema->groups[groupPosToSelect]->estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(filter));
}

uint32_t Enumerator::appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(
    Expression& expression, LogicalPlan& plan) {
    appendScanPropertiesIfNecessary(expression, plan);
    if (expression.hasSubqueryExpression()) {
        auto expressions = expression.getDependentSubqueryExpressions();
        for (auto& expr : expressions) {
            auto& subqueryExpression = (ExistentialSubqueryExpression&)*expr;
            if (!subqueryExpression.hasSubPlan()) {
                planSubquery(subqueryExpression, plan);
            }
        }
    }
    auto unFlatGroupPos = getUnFlatGroupsPos(expression, *plan.schema);
    return !unFlatGroupPos.empty() ? appendFlattensButOne(unFlatGroupPos, plan) :
                                     getAnyGroupPos(expression, *plan.schema);
}

void Enumerator::appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan) {
    for (auto& expr : expression.getDependentProperties()) {
        auto& propertyExpression = (PropertyExpression&)*expr;
        // skip properties that has been matched
        if (plan.schema->containVariable(propertyExpression.getInternalName())) {
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
    if (!plan.schema->containVariable(nodeExpression.getIDProperty())) {
        return;
    }
    auto scanProperty = make_shared<LogicalScanNodeProperty>(nodeExpression.getIDProperty(),
        nodeExpression.label, propertyExpression.getInternalName(), propertyExpression.propertyKey,
        UNSTRUCTURED == propertyExpression.dataType, plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(nodeExpression.getIDProperty());
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendScanRelPropertyIfNecessary(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& relExpression = (const RelExpression&)*propertyExpression.children[0];
    if (!plan.schema->containLogicalExtend(relExpression.getInternalName())) {
        return;
    }
    auto extend = plan.schema->getExistingLogicalExtend(relExpression.getInternalName());
    auto scanProperty = make_shared<LogicalScanRelProperty>(extend->boundNodeID,
        extend->boundNodeLabel, extend->nbrNodeID, extend->relLabel, extend->direction,
        propertyExpression.getInternalName(), propertyExpression.propertyKey, extend->isColumn,
        plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(extend->nbrNodeID);
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

vector<unique_ptr<LogicalPlan>> Enumerator::getInitialEmptyPlans() {
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    return plans;
}

unordered_set<uint32_t> Enumerator::getUnFlatGroupsPos(
    Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getDependentLeafExpressions();
    unordered_set<uint32_t> unFlatGroupsPos;
    for (auto& subExpression : subExpressions) {
        if (schema.containVariable(subExpression->getInternalName())) {
            auto groupPos = schema.getGroupPos(subExpression->getInternalName());
            if (!schema.groups[groupPos]->isFlat) {
                unFlatGroupsPos.insert(groupPos);
            }
        } else if (subExpression->expressionType == ALIAS) {
            for (auto& unFlatGroupPos : getUnFlatGroupsPos(*subExpression->children[0], schema)) {
                unFlatGroupsPos.insert(unFlatGroupPos);
            }
        }
    }
    return unFlatGroupsPos;
}

uint32_t Enumerator::getAnyGroupPos(Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getDependentLeafExpressions();
    GF_ASSERT(!subExpressions.empty());
    return schema.getGroupPos(subExpressions[0]->getInternalName());
}

} // namespace planner
} // namespace graphflow
