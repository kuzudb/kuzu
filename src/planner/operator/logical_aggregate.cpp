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
    for (auto& expression : getAllKeys()) {
        for (auto groupPos : children[0]->getSchema()->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    if (hasDistinctAggregate()) {
        return FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, children[0]->getSchema());
    } else {
        return FlattenAllButOne::getGroupsPosToFlatten(dependentGroupsPos,
            children[0]->getSchema());
    }
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForAggregate() {
    if (hasDistinctAggregate()) {
        f_group_pos_set dependentGroupsPos;
        for (auto& expression : aggregates) {
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
    for (auto& expression : keys) {
        result += expression->toString() + ", ";
    }
    for (auto& expression : dependentKeys) {
        result += expression->toString() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : aggregates) {
        result += expression->toString() + ", ";
    }
    result += "]";
    return result;
}

bool LogicalAggregate::hasDistinctAggregate() {
    for (auto& expression : aggregates) {
        auto funcExpr = expression->constPtrCast<binder::AggregateFunctionExpression>();
        if (funcExpr->isDistinct()) {
            return true;
        }
    }
    return false;
}

void LogicalAggregate::insertAllExpressionsToGroupAndScope(f_group_pos groupPos) {
    for (auto& expression : keys) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : dependentKeys) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : aggregates) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
