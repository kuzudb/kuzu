#include "planner/logical_plan/logical_operator/logical_distinct.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

std::string LogicalDistinct::getExpressionsForPrinting() const {
    std::string result;
    for (auto& expression : expressionsToDistinct) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

void LogicalDistinct::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

std::unordered_set<f_group_pos> LogicalDistinctFactorizationSolver::getGroupsPosToFlatten(
    const binder::expression_vector& expressions, LogicalOperator* distinctChild) {
    std::unordered_set<f_group_pos> dependentGroupsPos;
    for (auto& expression : expressions) {
        for (auto groupPos : distinctChild->getSchema()->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    return FlattenAllFactorizationSolver::getGroupsPosToFlatten(
        dependentGroupsPos, distinctChild->getSchema());
}

} // namespace planner
} // namespace kuzu
