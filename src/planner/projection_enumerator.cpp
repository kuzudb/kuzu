#include "src/planner/include/projection_enumerator.h"

#include "src/binder/expression/include/function_expression.h"
#include "src/planner/include/enumerator.h"
#include "src/planner/logical_plan/logical_operator/include/logical_aggregate.h"
#include "src/planner/logical_plan/logical_operator/include/logical_distinct.h"
#include "src/planner/logical_plan/logical_operator/include/logical_limit.h"
#include "src/planner/logical_plan/logical_operator/include/logical_multiplcity_reducer.h"
#include "src/planner/logical_plan/logical_operator/include/logical_order_by.h"
#include "src/planner/logical_plan/logical_operator/include/logical_projection.h"
#include "src/planner/logical_plan/logical_operator/include/logical_skip.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

namespace graphflow {
namespace planner {

void ProjectionEnumerator::enumerateProjectionBody(
    const BoundProjectionBody& projectionBody, const vector<unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // e.g. RETURN 1, COUNT(2)
            auto expressions = projectionBody.getProjectionExpressions();
            Enumerator::appendExpressionsScan(expressions, *plan);
        }
        enumerateAggregate(projectionBody, *plan);
        enumerateOrderBy(projectionBody, *plan);
        enumerateProjection(projectionBody, *plan);
        enumerateSkipAndLimit(projectionBody, *plan);
    }
}

void ProjectionEnumerator::enumerateAggregate(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    auto expressionsToAggregate = getExpressionsToAggregate(projectionBody, *plan.getSchema());
    if (expressionsToAggregate.empty()) {
        return;
    }
    expression_vector expressionsToProject;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        if (expressionToAggregate->getChildren().empty()) { // skip COUNT(*)
            continue;
        }
        expressionsToProject.push_back(expressionToAggregate->getChild(0));
    }
    auto expressionsToGroupBy = getExpressionToGroupBy(projectionBody, *plan.getSchema());
    for (auto& expressionToGroupBy : expressionsToGroupBy) {
        expressionsToProject.push_back(expressionToGroupBy);
    }
    appendProjection(expressionsToProject, plan);
    appendAggregate(expressionsToGroupBy, expressionsToAggregate, plan);
}

void ProjectionEnumerator::enumerateOrderBy(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    if (!projectionBody.hasOrderByExpressions()) {
        return;
    }
    appendOrderBy(projectionBody.getOrderByExpressions(), projectionBody.getSortingOrders(), plan);
}

void ProjectionEnumerator::enumerateProjection(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    auto expressionsToProject = getExpressionsToProject(projectionBody, *plan.getSchema());
    appendProjection(expressionsToProject, plan);
    if (projectionBody.getIsDistinct()) {
        appendDistinct(expressionsToProject, plan);
    }
}

void ProjectionEnumerator::enumerateSkipAndLimit(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    if (!projectionBody.hasSkip() && !projectionBody.hasLimit()) {
        return;
    }
    appendMultiplicityReducer(plan);
    if (projectionBody.hasSkip()) {
        appendSkip(projectionBody.getSkipNumber(), plan);
    }
    if (projectionBody.hasLimit()) {
        appendLimit(projectionBody.getLimitNumber(), plan);
    }
}

static unordered_set<uint32_t> getDiscardedGroupsPos(unordered_set<uint32_t>& groupsPosBefore,
    unordered_set<uint32_t>& groupsPosAfter, uint32_t totalNumGroups) {
    unordered_set<uint32_t> discardGroupsPos;
    for (auto i = 0u; i < totalNumGroups; ++i) {
        if (groupsPosBefore.contains(i) && !groupsPosAfter.contains(i)) {
            discardGroupsPos.insert(i);
        }
    }
    return discardGroupsPos;
}

void ProjectionEnumerator::appendProjection(
    const expression_vector& expressionsToProject, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    expression_vector expressionsToReference;
    vector<uint32_t> expressionsToReferenceOutputPos;
    expression_vector expressionsToEvaluate;
    vector<uint32_t> expressionsToEvaluateOutputPos;
    for (auto& expression : expressionsToProject) {
        enumerator->planSubqueryIfNecessary(expression, plan);
        if (schema->isExpressionInScope(*expression)) {
            expressionsToReference.push_back(expression);
            expressionsToReferenceOutputPos.push_back(schema->getGroupPos(*expression));
        } else {
            expressionsToEvaluate.push_back(expression);
            auto dependentGroupsPos = schema->getDependentGroupsPos(expression);
            uint32_t outputPos;
            if (dependentGroupsPos.empty()) { // e.g. constant that does not depend on any input.
                outputPos = schema->createGroup();
                schema->flattenGroup(outputPos); // Mark group holding constant as flat.
            } else {
                outputPos = Enumerator::appendFlattensButOne(dependentGroupsPos, plan);
            }
            expressionsToEvaluateOutputPos.push_back(outputPos);
        }
    }
    auto groupsPosInScopeBeforeProjection = schema->getGroupsPosInScope();
    schema->clearExpressionsInScope();
    for (auto i = 0u; i < expressionsToReference.size(); ++i) {
        // E.g. RETURN MIN(a.age), MAX(a.age). We first project each expression in aggregate. So
        // a.age might be projected twice.
        if (schema->isExpressionInScope(*expressionsToReference[i])) {
            continue;
        }
        schema->insertToScope(expressionsToReference[i], expressionsToReferenceOutputPos[i]);
    }
    for (auto i = 0u; i < expressionsToEvaluate.size(); ++i) {
        schema->insertToGroupAndScope(expressionsToEvaluate[i], expressionsToEvaluateOutputPos[i]);
    }
    auto groupsPosInScopeAfterProjection = schema->getGroupsPosInScope();
    auto discardGroupsPos = getDiscardedGroupsPos(
        groupsPosInScopeBeforeProjection, groupsPosInScopeAfterProjection, schema->getNumGroups());
    auto projection = make_shared<LogicalProjection>(
        expressionsToProject, std::move(discardGroupsPos), plan.getLastOperator());
    plan.appendOperator(std::move(projection));
}

void ProjectionEnumerator::appendDistinct(
    const expression_vector& expressionsToDistinct, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    for (auto& expression : expressionsToDistinct) {
        auto dependentGroupsPos = schema->getDependentGroupsPos(expression);
        enumerator->appendFlattens(dependentGroupsPos, plan);
    }
    auto distinct =
        make_shared<LogicalDistinct>(expressionsToDistinct, schema->copy(), plan.getLastOperator());
    schema->clear();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    plan.appendOperator(move(distinct));
}

void ProjectionEnumerator::appendAggregate(const expression_vector& expressionsToGroupBy,
    const expression_vector& expressionsToAggregate, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    bool hasDistinctFunc = false;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        auto& functionExpression = (AggregateFunctionExpression&)*expressionToAggregate;
        if (functionExpression.isDistinct()) {
            hasDistinctFunc = true;
        }
    }
    if (hasDistinctFunc) {
        for (auto& expressionToGroupBy : expressionsToGroupBy) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToGroupBy);
            enumerator->appendFlattens(dependentGroupsPos, plan);
        }
        for (auto& expressionToAggregate : expressionsToAggregate) {
            assert(isExpressionAggregate(expressionToAggregate->expressionType));
            auto& functionExpression = (AggregateFunctionExpression&)*expressionToAggregate;
            if (functionExpression.isDistinct()) {
                auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToAggregate);
                enumerator->appendFlattens(dependentGroupsPos, plan);
            }
        }
    } else {
        unordered_set<uint32_t> groupByPoses;
        for (auto& expressionToGroupBy : expressionsToGroupBy) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToGroupBy);
            groupByPoses.insert(dependentGroupsPos.begin(), dependentGroupsPos.end());
        }
        Enumerator::appendFlattensButOne(groupByPoses, plan);

        unordered_set<uint32_t> aggPoses;
        for (auto& expressionToAggregate : expressionsToAggregate) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToAggregate);
            aggPoses.insert(dependentGroupsPos.begin(), dependentGroupsPos.end());
        }
        Enumerator::appendFlattensButOne(aggPoses, plan);
    }
    auto aggregate = make_shared<LogicalAggregate>(
        expressionsToGroupBy, expressionsToAggregate, schema->copy(), plan.getLastOperator());
    schema->clear();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    plan.appendOperator(move(aggregate));
}

void ProjectionEnumerator::appendOrderBy(
    const expression_vector& expressions, const vector<bool>& isAscOrders, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    for (auto& expression : expressions) {
        enumerator->planSubqueryIfNecessary(expression, plan);
        // We only allow orderby key(s) to be unflat, if they are all part of the same factorization
        // group and there is no other factorized group in the schema, so any payload is also unflat
        // and part of the same factorization group. The rationale for this limitation is this: (1)
        // to keep both the frontend and orderby operators simpler, we want order by to not change
        // the schema, so the input and output of order by should have the same factorization
        // structure. (2) Because orderby needs to flatten the keys to sort, if a key column that is
        // unflat is the input, we need to somehow flatten it in the factorized table. However
        // whenever we can we want to avoid adding an explicit flatten operator as this makes us
        // fall back to tuple-at-a-time processing. However in the specified limited case, we can
        // give factorized table a set of unflat vectors (all in the same datachunk/factorization
        // group), sort the table, and scan into unflat vectors, so the schema remains the same. In
        // more complicated cases, e.g., when there are 2 factorization groups, FactorizedTable
        // cannot read back a flat column into an unflat vector.
        if (schema->getNumGroups() > 1) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expression);
            Enumerator::appendFlattens(dependentGroupsPos, plan);
        }
    }
    auto schemaBeforeOrderBy = schema->copy();
    SinkOperatorUtil::reComputeSchema(*schemaBeforeOrderBy, *schema);
    auto orderBy = make_shared<LogicalOrderBy>(expressions, isAscOrders,
        schemaBeforeOrderBy->getExpressionsInScope(), schemaBeforeOrderBy->copy(),
        plan.getLastOperator());
    plan.appendOperator(move(orderBy));
}

void ProjectionEnumerator::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.appendOperator(make_shared<LogicalMultiplicityReducer>(plan.getLastOperator()));
}

void ProjectionEnumerator::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPosToSelect = enumerator->appendFlattensButOne(schema->getGroupsPosInScope(), plan);
    auto limit = make_shared<LogicalLimit>(
        limitNumber, groupPosToSelect, schema->getGroupsPosInScope(), plan.getLastOperator());
    plan.setCardinality(limitNumber);
    plan.appendOperator(move(limit));
}

void ProjectionEnumerator::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPosToSelect = enumerator->appendFlattensButOne(schema->getGroupsPosInScope(), plan);
    auto skip = make_shared<LogicalSkip>(
        skipNumber, groupPosToSelect, schema->getGroupsPosInScope(), plan.getLastOperator());
    plan.setCardinality(plan.getCardinality() - skipNumber);
    plan.appendOperator(move(skip));
}

expression_vector ProjectionEnumerator::getExpressionToGroupBy(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    expression_vector expressionsToGroupBy;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (getSubAggregateExpressionsNotInScope(expression, schema).empty()) {
            expressionsToGroupBy.push_back(expression);
        }
    }
    return expressionsToGroupBy;
}

expression_vector ProjectionEnumerator::getExpressionsToAggregate(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    expression_vector expressionsToAggregate;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        for (auto& aggExpression : getSubAggregateExpressionsNotInScope(expression, schema)) {
            expressionsToAggregate.push_back(aggExpression);
        }
    }
    return expressionsToAggregate;
}

expression_vector ProjectionEnumerator::getExpressionsToProject(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    expression_vector expressionsToProject;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (expression->expressionType == VARIABLE) {
            for (auto& property : rewriteVariableAsAllPropertiesInScope(*expression, schema)) {
                expressionsToProject.push_back(property);
            }
        } else {
            expressionsToProject.push_back(expression);
        }
    }
    return expressionsToProject;
}

expression_vector ProjectionEnumerator::getSubAggregateExpressionsNotInScope(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    expression_vector result;
    if (schema.isExpressionInScope(*expression)) {
        return result;
    }
    if (expression->expressionType == AGGREGATE_FUNCTION) {
        result.push_back(expression);
        // Since aggregate expressions cannot be nested, we can safely return when current
        // expression is an aggregate expression.
        return result;
    }
    for (auto& child : expression->getChildren()) {
        for (auto& expr : getSubAggregateExpressionsNotInScope(child, schema)) {
            result.push_back(expr);
        }
    }
    return result;
}

expression_vector ProjectionEnumerator::rewriteVariableAsAllPropertiesInScope(
    const Expression& variable, const Schema& schema) {
    expression_vector result;
    for (auto& expression : schema.getExpressionsInScope()) {
        if (expression->expressionType == PROPERTY &&
            expression->getChild(0)->getUniqueName() == variable.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace planner
} // namespace graphflow
