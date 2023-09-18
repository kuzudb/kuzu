#include "planner/operator/logical_aggregate.h"

#include "binder/expression/function_expression.h"
#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

using namespace factorization;

void LogicalAggregate::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    insertAllExpressionsToGroupAndScope(groupPos);
}

void LogicalAggregate::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    insertAllExpressionsToGroupAndScope(0 /* groupPos */);
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForGroupBy() {
    f_group_pos_set dependentGroupsPos;
    for (auto& expression : getAllKeyExpressions()) {
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
    if (hasDistinctAggregate()) {
        f_group_pos_set dependentGroupsPos;
        for (auto& expression : aggregateExpressions) {
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
    for (auto& expression : keyExpressions) {
        result += expression->toString() + ", ";
    }
    for (auto& expression : dependentKeyExpressions) {
        result += expression->toString() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : aggregateExpressions) {
        result += expression->toString() + ", ";
    }
    result += "]";
    return result;
}

bool LogicalAggregate::hasDistinctAggregate() {
    for (auto& expression : aggregateExpressions) {
        auto& functionExpression = (binder::AggregateFunctionExpression&)*expression;
        if (functionExpression.isDistinct()) {
            return true;
        }
    }
    return false;
}

void LogicalAggregate::insertAllExpressionsToGroupAndScope(f_group_pos groupPos) {
    for (auto& expression : keyExpressions) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : dependentKeyExpressions) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : aggregateExpressions) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
