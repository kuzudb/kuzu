#include "src/planner/include/projection_enumerator.h"

#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/multiplicity_reducer/logical_multiplcity_reducer.h"
#include "src/planner/include/logical_plan/operator/order_by/logical_order_by.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"

namespace graphflow {
namespace planner {

void ProjectionEnumerator::enumerateProjectionBody(const BoundProjectionBody& projectionBody,
    const shared_ptr<Expression>& projectionBodyPredicate,
    const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn) {
    for (auto& plan : plans) {
        auto expressionsToAggregate = getExpressionsToAggregate(projectionBody, *plan->schema);
        if (!expressionsToAggregate.empty()) {
            appendAggregate(getExpressionToGroupBy(projectionBody, *plan->schema),
                expressionsToAggregate, *plan);
        }
        if (projectionBody.hasOrderByExpressions()) {
            appendOrderBy(
                projectionBody.getOrderByExpressions(), projectionBody.getSortingOrders(), *plan);
        }
        auto expressionsToProject = getExpressionsToProject(projectionBody, isFinalReturn);
        appendProjection(expressionsToProject, *plan);
        plan->schema->expressionsToCollect = expressionsToProject;
        if (projectionBodyPredicate) {
            enumerator->appendFilter(projectionBodyPredicate, *plan);
        }
        if (projectionBody.hasSkip() || projectionBody.hasLimit()) {
            appendMultiplicityReducer(*plan);
            if (projectionBody.hasSkip()) {
                appendSkip(projectionBody.getSkipNumber(), *plan);
            }
            if (projectionBody.hasLimit()) {
                appendLimit(projectionBody.getLimitNumber(), *plan);
            }
        }
    }
}

void ProjectionEnumerator::appendProjection(
    const vector<shared_ptr<Expression>>& expressionsToProject, LogicalPlan& plan) {
    unordered_set<uint32_t> nonDiscardGroupsPos;
    for (auto& expression : expressionsToProject) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        uint32_t groupPosToWrite = enumerator->appendFlattensButOne(dependentGroupsPos, plan);
        nonDiscardGroupsPos.insert(groupPosToWrite);
        plan.schema->getGroup(groupPosToWrite)->insertExpression(expression->getUniqueName());
    }

    // We collect the discarded group positions in the input data chunks to obtain their
    // multiplicity in the projection operation.
    vector<uint32_t> discardedGroupsPos;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        if (!nonDiscardGroupsPos.contains(i)) {
            discardedGroupsPos.push_back(i);
        }
    }

    auto projection = make_shared<LogicalProjection>(
        expressionsToProject, move(discardedGroupsPos), plan.lastOperator);
    plan.appendOperator(move(projection));
}

void ProjectionEnumerator::appendAggregate(
    const vector<shared_ptr<Expression>>& expressionsToGroupBy,
    const vector<shared_ptr<Expression>>& expressionsToAggregate, LogicalPlan& plan) {
    for (auto& expression : expressionsToGroupBy) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        enumerator->appendFlattens(dependentGroupsPos, plan);
    }
    for (auto& expression : expressionsToAggregate) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        enumerator->appendFlattensButOne(dependentGroupsPos, plan);
    }
    auto aggregate = make_shared<LogicalAggregate>(
        expressionsToGroupBy, expressionsToAggregate, plan.schema->copy(), plan.lastOperator);
    plan.schema->clearGroups();
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        plan.schema->insertToGroup(expression->getUniqueName(), groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        plan.schema->insertToGroup(expression->getUniqueName(), groupPos);
    }
    plan.appendOperator(move(aggregate));
}

void ProjectionEnumerator::appendOrderBy(const vector<shared_ptr<Expression>>& expressions,
    const vector<bool>& isAscOrders, LogicalPlan& plan) {
    for (auto& expression : expressions) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        enumerator->appendFlattens(dependentGroupsPos, plan);
    }
    auto orderBy = make_shared<LogicalOrderBy>(expressions, isAscOrders, plan.lastOperator);
    plan.appendOperator(move(orderBy));
}

void ProjectionEnumerator::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.appendOperator(make_shared<LogicalMultiplicityReducer>(plan.lastOperator));
}

void ProjectionEnumerator::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto groupPosToSelect = enumerator->appendFlattensButOne(plan.schema->getGroupsPos(), plan);
    // Because our resultSet is shared through the plan and limit might not appear at the end (due
    // to WITH clause), limit needs to know how many tuples are available under it. So it requires a
    // subset of dataChunks that may different from shared resultSet.
    vector<uint32_t> groupsPosToLimit;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        groupsPosToLimit.push_back(i);
    }
    auto limit = make_shared<LogicalLimit>(
        limitNumber, groupPosToSelect, move(groupsPosToLimit), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality = limitNumber;
    }
    plan.appendOperator(move(limit));
}

void ProjectionEnumerator::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto groupPosToSelect = enumerator->appendFlattensButOne(plan.schema->getGroupsPos(), plan);
    vector<uint32_t> groupsPosToSkip;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        groupsPosToSkip.push_back(i);
    }
    auto skip = make_shared<LogicalSkip>(
        skipNumber, groupPosToSelect, move(groupsPosToSkip), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality -= skipNumber;
    }
    plan.appendOperator(move(skip));
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionToGroupBy(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    vector<shared_ptr<Expression>> expressionsToGroupBy;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (Enumerator::getAggregationExpressionsNotInSchema(expression, schema).empty()) {
            expressionsToGroupBy.push_back(expression);
        }
    }
    return expressionsToGroupBy;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionsToAggregate(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    vector<shared_ptr<Expression>> expressionsToAggregate;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        for (auto& aggExpression :
            Enumerator::getAggregationExpressionsNotInSchema(expression, schema)) {
            expressionsToAggregate.push_back(aggExpression);
        }
    }
    return expressionsToAggregate;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionsToProject(
    const BoundProjectionBody& projectionBody, bool isRewritingAllProperties) {
    vector<shared_ptr<Expression>> expressionsToProject;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (expression->expressionType == VARIABLE) {
            for (auto& property : rewriteVariableExpression(expression, isRewritingAllProperties)) {
                expressionsToProject.push_back(property);
            }
        } else {
            expressionsToProject.push_back(expression);
        }
    }
    return expressionsToProject;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteVariableExpression(
    const shared_ptr<Expression>& variable, bool isRewritingAllProperties) {
    GF_ASSERT(VARIABLE == variable->expressionType);
    vector<shared_ptr<Expression>> result;
    if (NODE == variable->dataType) {
        auto node = static_pointer_cast<NodeExpression>(variable);
        if (isRewritingAllProperties) {
            for (auto& expression : rewriteNodeExpression(node)) {
                result.push_back(expression);
            }
        } else {
            result.push_back(node->getNodeIDPropertyExpression());
        }
        return result;
    } else if (REL == variable->dataType) {
        auto rel = static_pointer_cast<RelExpression>(variable);
        if (isRewritingAllProperties) {
            for (auto& expression : rewriteRelExpression(rel)) {
                result.push_back(expression);
            }
        }
        return result;
    }
    assert(false);
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteNodeExpression(
    const shared_ptr<NodeExpression>& node) {
    vector<shared_ptr<Expression>> result;
    for (auto& property :
        createPropertyExpressions(node, catalog.getAllNodeProperties(node->label))) {
        result.push_back(property);
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteRelExpression(
    const shared_ptr<RelExpression>& rel) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : createPropertyExpressions(rel, catalog.getRelProperties(rel->label))) {
        result.push_back(property);
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::createPropertyExpressions(
    const shared_ptr<Expression>& variable, const vector<PropertyDefinition>& properties) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : properties) {
        result.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, variable));
    }
    return result;
}

} // namespace planner
} // namespace graphflow
