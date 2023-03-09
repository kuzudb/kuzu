#include "planner/logical_plan/logical_operator/logical_aggregate.h"

#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

using namespace factorization;

void LogicalAggregate::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

void LogicalAggregate::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        schema->insertToGroupAndScope(expression, 0);
    }
    for (auto& expression : expressionsToAggregate) {
        schema->insertToGroupAndScope(expression, 0);
    }
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForGroupBy() {
    f_group_pos_set dependentGroupsPos;
    for (auto& expression : expressionsToGroupBy) {
        for (auto groupPos : children[0]->getSchema()->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    if (hasDistinctAggregate()) {
        return FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, children[0]->getSchema());
    } else {
        return FlattenAllButOne::getGroupsPosToFlatten(
            dependentGroupsPos, children[0]->getSchema());
    }
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForAggregate() {
    if (hasDistinctAggregate() || expressionsToAggregate.size() > 1) {
        f_group_pos_set dependentGroupsPos;
        for (auto& expression : expressionsToAggregate) {
            for (auto groupPos : children[0]->getSchema()->getDependentGroupsPos(expression)) {
                dependentGroupsPos.insert(groupPos);
            }
        }
        return FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, children[0]->getSchema());
    }
    return f_group_pos_set{};
}

std::string LogicalAggregate::getExpressionsForPrinting() const {
    std::string result = "Group By [";
    for (auto& expression : expressionsToGroupBy) {
        result += expression->toString() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : expressionsToAggregate) {
        result += expression->toString() + ", ";
    }
    result += "]";
    return result;
}

bool LogicalAggregate::hasDistinctAggregate() {
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
