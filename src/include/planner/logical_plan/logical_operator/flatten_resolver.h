#pragma once

#include "planner/logical_plan/logical_operator/base_logical_operator.h"

namespace kuzu {
namespace planner {
namespace factorization {

struct FlattenAllButOne {
    static f_group_pos_set getGroupsPosToFlatten(const f_group_pos_set& groupsPos, Schema* schema);
};

struct FlattenAll {
    static f_group_pos_set getGroupsPosToFlatten(const f_group_pos_set& groupsPos, Schema* schema);
};

} // namespace factorization
} // namespace planner
} // namespace kuzu
