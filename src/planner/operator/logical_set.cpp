#include "planner/logical_plan/logical_operator/logical_set.h"

#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalSetRelProperty::getGroupsPosToFlatten(uint32_t setItemIdx) {
    f_group_pos_set result;
    auto rel = rels[setItemIdx];
    auto rhs = setItems[setItemIdx].second;
    auto childSchema = children[0]->getSchema();
    result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
    result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
    for (auto groupPos : childSchema->getDependentGroupsPos(rhs)) {
        result.insert(groupPos);
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

} // namespace planner
} // namespace kuzu
