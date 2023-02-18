#include "planner/logical_plan/logical_operator/logical_unwind.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

void LogicalUnwind::computeSchema() {
    copyChildSchema(0);
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(aliasExpression, groupPos);
}

std::unordered_set<f_group_pos> LogicalUnwindFactorizationSolver::getGroupsPosToFlatten(
    const std::shared_ptr<binder::Expression>& expression, LogicalOperator* unwindChild) {
    auto dependentGroupsPos = unwindChild->getSchema()->getDependentGroupsPos(expression);
    return FlattenAllFactorizationSolver::getGroupsPosToFlatten(
        dependentGroupsPos, unwindChild->getSchema());
}

} // namespace planner
} // namespace kuzu
