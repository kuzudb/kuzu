#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {
namespace factorization {

f_group_pos_set FlattenAllButOne::getGroupsPosToFlatten(
    const f_group_pos_set& groupsPos, Schema* schema) {
    std::vector<f_group_pos> unFlatGroupsPos;
    for (auto groupPos : groupsPos) {
        if (!schema->getGroup(groupPos)->isFlat()) {
            unFlatGroupsPos.push_back(groupPos);
        }
    }
    f_group_pos_set result;
    // Keep the first group as unFlat.
    for (auto i = 1u; i < unFlatGroupsPos.size(); ++i) {
        result.insert(unFlatGroupsPos[i]);
    }
    return result;
}

f_group_pos_set FlattenAll::getGroupsPosToFlatten(
    const f_group_pos_set& groupsPos, Schema* schema) {
    f_group_pos_set result;
    for (auto groupPos : groupsPos) {
        if (!schema->getGroup(groupPos)->isFlat()) {
            result.insert(groupPos);
        }
    }
    return result;
}

} // namespace factorization
} // namespace planner
} // namespace kuzu
