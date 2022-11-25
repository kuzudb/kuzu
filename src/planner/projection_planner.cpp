#include "planner/projection_planner.h"

#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "planner/logical_plan/logical_operator/logical_limit.h"
#include "planner/logical_plan/logical_operator/logical_multiplcity_reducer.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_skip.h"
#include "planner/logical_plan/logical_operator/sink_util.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void ProjectionPlanner::planProjectionBody(
    const BoundProjectionBody& projectionBody, const vector<unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planProjectionBody(projectionBody, *plan);
    }
}

void ProjectionPlanner::planProjectionBody(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    if (plan.isEmpty()) { // e.g. RETURN 1, COUNT(2)
        expression_vector expressionsToScan;
        for (auto& expression : projectionBody.getProjectionExpressions()) {
            if (expression->expressionType == AGGREGATE_FUNCTION) { // aggregate on const
                if (expression->getNumChildren() != 0) {            // skip count(*)
                    expressionsToScan.push_back(expression->getChild(0));
                }
            } else {
                expressionsToScan.push_back(expression);
            }
        }
        QueryPlanner::appendExpressionsScan(expressionsToScan, plan);
    }
    // NOTE: As a temporary solution, we rewrite variables in WITH clause as all properties in scope
    // during planning stage. The purpose is to avoid reading unnecessary properties for WITH.
    // E.g. MATCH (a) WITH a RETURN a.age -> MATCH (a) WITH a.age RETURN a.age
    // This rewrite should be removed once we add an optimizer that can remove unnecessary columns.
    auto expressionsToProject =
        rewriteExpressionsToProject(projectionBody.getProjectionExpressions(), *schema);
    auto expressionsToAggregate = getExpressionsToAggregate(expressionsToProject, *schema);
    auto expressionsToGroupBy = getExpressionToGroupBy(expressionsToProject, *schema);
    if (!expressionsToAggregate.empty()) {
        planAggregate(expressionsToAggregate, expressionsToGroupBy, plan);
    }
    if (projectionBody.hasOrderByExpressions()) {
        appendOrderBy(
            projectionBody.getOrderByExpressions(), projectionBody.getSortingOrders(), plan);
    }
    appendProjection(expressionsToProject, plan);
    if (projectionBody.getIsDistinct()) {
        QueryPlanner::appendDistinct(expressionsToProject, plan);
    }
    if (projectionBody.hasSkipOrLimit()) {
        appendMultiplicityReducer(plan);
        if (projectionBody.hasSkip()) {
            appendSkip(projectionBody.getSkipNumber(), plan);
        }
        if (projectionBody.hasLimit()) {
            appendLimit(projectionBody.getLimitNumber(), plan);
        }
    }
}

void ProjectionPlanner::planAggregate(const expression_vector& expressionsToAggregate,
    const expression_vector& expressionsToGroupBy, LogicalPlan& plan) {
    assert(!expressionsToAggregate.empty());
    expression_vector expressionsToProject;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        if (expressionToAggregate->getChildren().empty()) { // skip COUNT(*)
            continue;
        }
        expressionsToProject.push_back(expressionToAggregate->getChild(0));
    }
    for (auto& expressionToGroupBy : expressionsToGroupBy) {
        expressionsToProject.push_back(expressionToGroupBy);
    }
    appendProjection(expressionsToProject, plan);
    appendAggregate(expressionsToGroupBy, expressionsToAggregate, plan);
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

void ProjectionPlanner::appendProjection(
    const expression_vector& expressionsToProject, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    expression_vector expressionsToReference;
    vector<uint32_t> expressionsToReferenceOutputPos;
    expression_vector expressionsToEvaluate;
    vector<uint32_t> expressionsToEvaluateOutputPos;
    for (auto& expression : expressionsToProject) {
        queryPlanner->planSubqueryIfNecessary(expression, plan);
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
                outputPos = QueryPlanner::appendFlattensButOne(dependentGroupsPos, plan);
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
    plan.setLastOperator(std::move(projection));
}

void ProjectionPlanner::appendAggregate(const expression_vector& expressionsToGroupBy,
    const expression_vector& expressionsToAggregate, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    bool hasDistinctFunc = false;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        auto& functionExpression = (AggregateFunctionExpression&)*expressionToAggregate;
        if (functionExpression.isDistinct()) {
            hasDistinctFunc = true;
        }
    }
    if (hasDistinctFunc) { // Flatten all inputs.
        for (auto& expressionToGroupBy : expressionsToGroupBy) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToGroupBy);
            QueryPlanner::appendFlattens(dependentGroupsPos, plan);
        }
        for (auto& expressionToAggregate : expressionsToAggregate) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToAggregate);
            QueryPlanner::appendFlattens(dependentGroupsPos, plan);
        }
    } else {
        // Flatten all but one for ALL group by keys.
        unordered_set<uint32_t> groupByPoses;
        for (auto& expressionToGroupBy : expressionsToGroupBy) {
            auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToGroupBy);
            groupByPoses.insert(dependentGroupsPos.begin(), dependentGroupsPos.end());
        }
        QueryPlanner::appendFlattensButOne(groupByPoses, plan);
        if (expressionsToAggregate.size() > 1) {
            for (auto& expressionToAggregate : expressionsToAggregate) {
                auto dependentGroupsPos = schema->getDependentGroupsPos(expressionToAggregate);
                QueryPlanner::appendFlattens(dependentGroupsPos, plan);
            }
        }
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
    plan.setLastOperator(move(aggregate));
}

void ProjectionPlanner::appendOrderBy(
    const expression_vector& expressions, const vector<bool>& isAscOrders, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    for (auto& expression : expressions) {
        queryPlanner->planSubqueryIfNecessary(expression, plan);
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
            QueryPlanner::appendFlattens(dependentGroupsPos, plan);
        }
    }
    auto schemaBeforeOrderBy = schema->copy();
    SinkOperatorUtil::recomputeSchema(*schemaBeforeOrderBy, *schema);
    auto orderBy = make_shared<LogicalOrderBy>(expressions, isAscOrders,
        schemaBeforeOrderBy->getExpressionsInScope(), schemaBeforeOrderBy->copy(),
        plan.getLastOperator());
    plan.setLastOperator(move(orderBy));
}

void ProjectionPlanner::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.setLastOperator(make_shared<LogicalMultiplicityReducer>(plan.getLastOperator()));
}

void ProjectionPlanner::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPosToSelect = QueryPlanner::appendFlattensButOne(schema->getGroupsPosInScope(), plan);
    auto limit = make_shared<LogicalLimit>(
        limitNumber, groupPosToSelect, schema->getGroupsPosInScope(), plan.getLastOperator());
    plan.setCardinality(limitNumber);
    plan.setLastOperator(move(limit));
}

void ProjectionPlanner::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPosToSelect = QueryPlanner::appendFlattensButOne(schema->getGroupsPosInScope(), plan);
    auto skip = make_shared<LogicalSkip>(
        skipNumber, groupPosToSelect, schema->getGroupsPosInScope(), plan.getLastOperator());
    plan.setCardinality(plan.getCardinality() - skipNumber);
    plan.setLastOperator(move(skip));
}

expression_vector ProjectionPlanner::getExpressionToGroupBy(
    const expression_vector& expressionsToProject, const Schema& schema) {
    expression_vector expressionsToGroupBy;
    for (auto& expression : expressionsToProject) {
        if (getSubAggregateExpressionsNotInScope(expression, schema).empty()) {
            expressionsToGroupBy.push_back(expression);
        }
    }
    return expressionsToGroupBy;
}

expression_vector ProjectionPlanner::getExpressionsToAggregate(
    const expression_vector& expressionsToProject, const Schema& schema) {
    expression_vector expressionsToAggregate;
    for (auto& expression : expressionsToProject) {
        for (auto& aggExpression : getSubAggregateExpressionsNotInScope(expression, schema)) {
            expressionsToAggregate.push_back(aggExpression);
        }
    }
    return expressionsToAggregate;
}

expression_vector ProjectionPlanner::rewriteExpressionsToProject(
    const expression_vector& expressionsToProject, const Schema& schema) {
    expression_vector result;
    for (auto& expression : expressionsToProject) {
        if (expression->dataType.typeID == NODE || expression->dataType.typeID == REL) {
            for (auto& property : rewriteVariableAsAllPropertiesInScope(*expression, schema)) {
                result.push_back(property);
            }
        } else {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector ProjectionPlanner::getSubAggregateExpressionsNotInScope(
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

expression_vector ProjectionPlanner::rewriteVariableAsAllPropertiesInScope(
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
} // namespace kuzu
