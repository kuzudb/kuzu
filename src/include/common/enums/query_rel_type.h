#pragma once

#include <cstdint>

#include "path_semantic.h"

namespace kuzu {
namespace common {

enum class QueryRelType : uint8_t {
    NON_RECURSIVE = 0,
    VARIABLE_LENGTH_WALK = 1,
    VARIABLE_LENGTH_TRAIL = 2,
    VARIABLE_LENGTH_ACYCLIC = 3,
    SHORTEST = 4,
    ALL_SHORTEST = 5,
};

struct QueryRelTypeUtils {
    static bool isRecursive(QueryRelType queryRelType) {
        return queryRelType != QueryRelType::NON_RECURSIVE;
    }

    static PathSemantic getPathSemantic(QueryRelType queryRelType);
};

} // namespace common
} // namespace kuzu
