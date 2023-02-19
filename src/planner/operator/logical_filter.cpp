#include "planner/logical_plan/logical_operator/logical_filter.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos LogicalFilter::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(expression);
    SchemaUtils::validateAtMostOneUnFlatGroup(dependentGroupsPos, *childSchema);
    return SchemaUtils::getLeadingGroupPos(dependentGroupsPos, *childSchema);
}

std::unordered_set<f_group_pos> LogicalFilterFactorizationSolver::getGroupsPosToFlatten(
    const std::shared_ptr<binder::Expression>& expression, LogicalOperator* filterChild) {
    auto dependentGroupsPos = filterChild->getSchema()->getDependentGroupsPos(expression);
    return FlattenAllButOneFactorizationResolver::getGroupsPosToFlatten(
        dependentGroupsPos, filterChild->getSchema());
}

} // namespace planner
} // namespace kuzu
