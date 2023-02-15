#include "planner/logical_plan/logical_operator/logical_filter.h"

namespace kuzu {
namespace planner {

f_group_pos LogicalFilter::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(expression);
    SchemaUtils::validateAtMostOneUnFlatGroup(dependentGroupsPos, *childSchema);
    return SchemaUtils::getLeadingGroupPos(dependentGroupsPos, *childSchema);
}

} // namespace planner
} // namespace kuzu
