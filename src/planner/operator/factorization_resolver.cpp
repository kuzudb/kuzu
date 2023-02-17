#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

std::unordered_set<f_group_pos> FlattenAllButOneFactorizationResolver::getGroupsPosToFlatten(
    const std::unordered_set<f_group_pos>& groupsPos, Schema* schema) {
    std::vector<f_group_pos> unFlatGroupsPos;
    for (auto groupPos : groupsPos) {
        if (!schema->getGroup(groupPos)->isFlat()) {
            unFlatGroupsPos.push_back(groupPos);
        }
    }
    std::unordered_set<f_group_pos> result;
    for (auto i = 1u; i < unFlatGroupsPos.size(); ++i) {
        result.insert(unFlatGroupsPos[i]);
    }
    return result;
}

} // namespace planner
} // namespace kuzu
