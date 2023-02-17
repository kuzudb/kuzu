#include "planner/logical_plan/logical_operator/base_logical_operator.h"

namespace kuzu {
namespace planner {

struct FlattenAllButOneFactorizationResolver {
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(
        const std::unordered_set<f_group_pos>& groupsPos, Schema* schema);
};

struct FlattenAllFactorizationSolver {
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(
        const std::unordered_set<f_group_pos>& groupsPos, Schema* schema);
};

} // namespace planner
} // namespace kuzu
