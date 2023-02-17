#include "planner/logical_plan/logical_operator/logical_aggregate.h"

#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

void LogicalAggregate::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

std::string LogicalAggregate::getExpressionsForPrinting() const {
    std::string result = "Group By [";
    for (auto& expression : expressionsToGroupBy) {
        result += expression->getUniqueName() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : expressionsToAggregate) {
        result += expression->getUniqueName() + ", ";
    }
    result += "]";
    return result;
}

std::unordered_set<f_group_pos>
LogicalAggregateFactorizationSolver::getGroupsPosToFlattenForGroupBy(
    const binder::expression_vector& expressionsToGroupBy,
    const binder::expression_vector& expressionsToAggregate, LogicalOperator* aggregateChild) {
    std::unordered_set<f_group_pos> dependentGroupsPos;
    for (auto& expression : expressionsToGroupBy) {
        for (auto groupPos : aggregateChild->getSchema()->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    if (hasDistinctAggregate(expressionsToAggregate)) {
        return FlattenAllFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, aggregateChild->getSchema());
    } else {
        return FlattenAllButOneFactorizationResolver::getGroupsPosToFlatten(
            dependentGroupsPos, aggregateChild->getSchema());
    }
}

std::unordered_set<f_group_pos>
LogicalAggregateFactorizationSolver::getGroupsPosToFlattenForAggregate(
    const binder::expression_vector& expressionsToAggregate, LogicalOperator* aggregateChild) {
    if (hasDistinctAggregate(expressionsToAggregate) || expressionsToAggregate.size() > 1) {
        std::unordered_set<f_group_pos> dependentGroupsPos;
        for (auto& expression : expressionsToAggregate) {
            for (auto groupPos : aggregateChild->getSchema()->getDependentGroupsPos(expression)) {
                dependentGroupsPos.insert(groupPos);
            }
        }
        return FlattenAllFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, aggregateChild->getSchema());
    }
    return std::unordered_set<f_group_pos>{};
}

bool LogicalAggregateFactorizationSolver::hasDistinctAggregate(
    const binder::expression_vector& expressionsToAggregate) {
    for (auto& expressionToAggregate : expressionsToAggregate) {
        auto& functionExpression = (binder::AggregateFunctionExpression&)*expressionToAggregate;
        if (functionExpression.isDistinct()) {
            return true;
        }
    }
    return false;
}

} // namespace planner
} // namespace kuzu
