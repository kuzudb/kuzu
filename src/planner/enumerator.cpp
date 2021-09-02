#include "src/planner/include/enumerator.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Enumerator::getBestPlan(const BoundSingleQuery& singleQuery) {
    auto plans = enumeratePlans(singleQuery);
    unique_ptr<LogicalPlan> bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->cost < bestPlan->cost) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans(const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = QueryNormalizer::normalizeQuery(singleQuery);
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    for (auto& queryPart : normalizedQuery->getQueryParts()) {
        plans = enumerateQueryPart(*queryPart, move(plans));
    }
    return plans;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumerateQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    auto plans = move(prevPlans);
    if (queryPart.hasLoadCSVStatement()) {
        assert(plans.size() == 1); // load csv must be the first clause
        appendLoadCSV(*queryPart.getLoadCSVStatement(), *plans[0]);
    }
    plans = joinOrderEnumerator.enumerateJoinOrder(queryPart, move(plans));
    projectionEnumerator.enumerateProjectionBody(
        *queryPart.getProjectionBody(), plans, queryPart.isLastQueryPart());
    return plans;
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

uint32_t Enumerator::appendFlattensIfNecessary(
    const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan) {
    // randomly pick the first unFlat group to select and flatten the rest
    auto groupPosToSelect = *unFlatGroupsPos.begin();
    for (auto& unFlatGroupPos : unFlatGroupsPos) {
        if (unFlatGroupPos != groupPosToSelect) {
            appendFlattenIfNecessary(unFlatGroupPos, plan);
        }
    }
    return groupPosToSelect;
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

void Enumerator::appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan) {
    appendScanPropertiesIfNecessary(*expression, plan);
    auto unFlatGroupsPos = getUnFlatGroupsPos(*expression, *plan.schema);
    uint32_t groupPosToSelect = unFlatGroupsPos.empty() ?
                                    getAnyGroupPos(*expression, *plan.schema) :
                                    appendFlattensIfNecessary(unFlatGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    plan.schema->groups[groupPosToSelect]->estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(filter));
}

void Enumerator::appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan) {
    for (auto& expr : expression.getDependentProperties()) {
        auto& propertyExpression = (PropertyExpression&)*expr;
        if (plan.schema->containVariable(propertyExpression.getInternalName())) {
            continue;
        }
        NODE == propertyExpression.children[0]->dataType ?
            appendScanNodeProperty(propertyExpression, plan) :
            appendScanRelProperty(propertyExpression, plan);
    }
}

void Enumerator::appendScanNodeProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& nodeExpression = (const NodeExpression&)*propertyExpression.children[0];
    auto scanProperty = make_shared<LogicalScanNodeProperty>(nodeExpression.getIDProperty(),
        nodeExpression.label, propertyExpression.getInternalName(), propertyExpression.propertyKey,
        UNSTRUCTURED == propertyExpression.dataType, plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(nodeExpression.getIDProperty());
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendScanRelProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& relExpression = (const RelExpression&)*propertyExpression.children[0];
    auto extend = plan.schema->getExistingLogicalExtend(relExpression.getInternalName());
    auto scanProperty = make_shared<LogicalScanRelProperty>(extend->boundNodeID,
        extend->boundNodeLabel, extend->nbrNodeID, extend->relLabel, extend->direction,
        propertyExpression.getInternalName(), propertyExpression.propertyKey, extend->isColumn,
        plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(extend->nbrNodeID);
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
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
        } else {
            GF_ASSERT(ALIAS == subExpression->expressionType);
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
