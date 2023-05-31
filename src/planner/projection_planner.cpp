#include "planner/projection_planner.h"

#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/flatten_resolver.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "planner/logical_plan/logical_operator/logical_limit.h"
#include "planner/logical_plan/logical_operator/logical_multiplcity_reducer.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_skip.h"
#include "planner/logical_plan/logical_operator/sink_util.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void ProjectionPlanner::planProjectionBody(const BoundProjectionBody& projectionBody,
    const std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planProjectionBody(projectionBody, *plan);
    }
}

void ProjectionPlanner::planProjectionBody(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
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
        queryPlanner->appendExpressionsScan(expressionsToScan, plan);
    }
    // NOTE: As a temporary solution, we rewrite variables in WITH clause as all properties in scope
    // during planning stage. The purpose is to avoid reading unnecessary properties for WITH.
    // E.g. MATCH (a) WITH a RETURN a.age -> MATCH (a) WITH a.age RETURN a.age
    // This rewrite should be removed once we add an optimizer that can remove unnecessary columns.
    auto expressionsToProject =
        rewriteExpressionsToProject(projectionBody.getProjectionExpressions(), *plan.getSchema());
    auto expressionsToAggregate =
        getExpressionsToAggregate(expressionsToProject, *plan.getSchema());
    auto expressionsToGroupBy = getExpressionToGroupBy(expressionsToProject, *plan.getSchema());
    if (!expressionsToAggregate.empty()) {
        planAggregate(expressionsToAggregate, expressionsToGroupBy, plan);
    }
    if (projectionBody.hasOrderByExpressions()) {
        planOrderBy(expressionsToProject, projectionBody.getOrderByExpressions(),
            projectionBody.getSortingOrders(), plan);
    }
    appendProjection(expressionsToProject, plan);
    if (projectionBody.getIsDistinct()) {
        queryPlanner->appendDistinct(expressionsToProject, plan);
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

void ProjectionPlanner::planOrderBy(const binder::expression_vector& expressionsToProject,
    const binder::expression_vector& expressionsToOrderBy, const std::vector<bool>& isAscOrders,
    kuzu::planner::LogicalPlan& plan) {
    auto expressionsToProjectBeforeOrderBy = expressionsToProject;
    auto expressionsToProjectSet =
        expression_set{expressionsToProject.begin(), expressionsToProject.end()};
    for (auto& expression : expressionsToOrderBy) {
        if (!expressionsToProjectSet.contains(expression)) {
            expressionsToProjectBeforeOrderBy.push_back(expression);
        }
    }
    appendProjection(expressionsToProjectBeforeOrderBy, plan);
    appendOrderBy(expressionsToOrderBy, isAscOrders, plan);
}

void ProjectionPlanner::appendProjection(
    const expression_vector& expressionsToProject, LogicalPlan& plan) {
    for (auto& expression : expressionsToProject) {
        queryPlanner->planSubqueryIfNecessary(expression, plan);
    }
    for (auto& expression : expressionsToProject) {
        auto dependentGroupsPos = plan.getSchema()->getDependentGroupsPos(expression);
        auto groupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
            dependentGroupsPos, plan.getSchema());
        queryPlanner->appendFlattens(groupsPosToFlatten, plan);
    }
    auto projection = make_shared<LogicalProjection>(expressionsToProject, plan.getLastOperator());
    projection->computeFactorizedSchema();
    plan.setLastOperator(std::move(projection));
}

void ProjectionPlanner::appendAggregate(const expression_vector& expressionsToGroupBy,
    const expression_vector& expressionsToAggregate, LogicalPlan& plan) {
    auto aggregate = make_shared<LogicalAggregate>(
        expressionsToGroupBy, expressionsToAggregate, plan.getLastOperator());
    queryPlanner->appendFlattens(aggregate->getGroupsPosToFlattenForGroupBy(), plan);
    aggregate->setChild(0, plan.getLastOperator());
    queryPlanner->appendFlattens(aggregate->getGroupsPosToFlattenForAggregate(), plan);
    aggregate->setChild(0, plan.getLastOperator());
    aggregate->computeFactorizedSchema();
    plan.setLastOperator(std::move(aggregate));
}

void ProjectionPlanner::appendOrderBy(
    const expression_vector& expressions, const std::vector<bool>& isAscOrders, LogicalPlan& plan) {
    auto orderBy = make_shared<LogicalOrderBy>(expressions, isAscOrders, plan.getLastOperator());
    queryPlanner->appendFlattens(orderBy->getGroupsPosToFlatten(), plan);
    orderBy->setChild(0, plan.getLastOperator());
    orderBy->computeFactorizedSchema();
    plan.setLastOperator(std::move(orderBy));
}

void ProjectionPlanner::appendMultiplicityReducer(LogicalPlan& plan) {
    auto multiplicityReducer = make_shared<LogicalMultiplicityReducer>(plan.getLastOperator());
    multiplicityReducer->computeFactorizedSchema();
    plan.setLastOperator(std::move(multiplicityReducer));
}

void ProjectionPlanner::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto limit = make_shared<LogicalLimit>(limitNumber, plan.getLastOperator());
    queryPlanner->appendFlattens(limit->getGroupsPosToFlatten(), plan);
    limit->setChild(0, plan.getLastOperator());
    limit->computeFactorizedSchema();
    plan.setCardinality(limitNumber);
    plan.setLastOperator(std::move(limit));
}

void ProjectionPlanner::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto skip = make_shared<LogicalSkip>(skipNumber, plan.getLastOperator());
    queryPlanner->appendFlattens(skip->getGroupsPosToFlatten(), plan);
    skip->setChild(0, plan.getLastOperator());
    skip->computeFactorizedSchema();
    plan.setCardinality(plan.getCardinality() - skipNumber);
    plan.setLastOperator(std::move(skip));
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
        switch (expression->getDataType().getLogicalTypeID()) {
        case LogicalTypeID::NODE:
        case LogicalTypeID::REL: {
            for (auto& property : rewriteVariableAsAllPropertiesInScope(*expression, schema)) {
                result.push_back(property);
            }
        } break;
        case LogicalTypeID::RECURSIVE_REL: {
            auto& rel = (RelExpression&)*expression;
            result.push_back(rel.getLengthExpression());
            result.push_back(expression);
        } break;
        default:
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector ProjectionPlanner::getSubAggregateExpressionsNotInScope(
    const std::shared_ptr<Expression>& expression, const Schema& schema) {
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
        if (expression->expressionType != common::PROPERTY) {
            continue;
        }
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (propertyExpression->getVariableName() == variable.getUniqueName()) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
