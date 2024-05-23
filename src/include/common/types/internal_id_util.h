#pragma once

#include "function/hash/hash_functions.h"
#include "internal_id_t.h"

namespace kuzu {
namespace common {

using internal_id_set_t = std::unordered_set<internalID_t, function::InternalIDHasher>;
using node_id_set_t = internal_id_set_t;
using rel_id_set_t = internal_id_set_t;
template<typename T>
using internal_id_map_t = std::unordered_map<internalID_t, T, function::InternalIDHasher>;
template<typename T>
using node_id_map_t = internal_id_map_t<T>;
template<typename T>
using rel_id_map_t = internal_id_map_t<T>;

} // namespace common
} // namespace kuzu
