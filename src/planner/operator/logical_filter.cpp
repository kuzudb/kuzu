#include "planner/operator/logical_filter.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalFilter::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(expression);
    return factorization::FlattenAllButOne::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

f_group_pos LogicalFilter::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(expression);
    SchemaUtils::validateAtMostOneUnFlatGroup(dependentGroupsPos, *childSchema);
    return SchemaUtils::getLeadingGroupPos(dependentGroupsPos, *childSchema);
}

} // namespace planner
} // namespace kuzu
