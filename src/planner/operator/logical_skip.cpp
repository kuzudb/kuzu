#include "planner/logical_plan/logical_skip.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalSkip::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAllButOne::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

f_group_pos LogicalSkip::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto groupsPosInScope = childSchema->getGroupsPosInScope();
    SchemaUtils::validateAtMostOneUnFlatGroup(groupsPosInScope, *childSchema);
    return SchemaUtils::getLeadingGroupPos(groupsPosInScope, *childSchema);
}

} // namespace planner
} // namespace kuzu
