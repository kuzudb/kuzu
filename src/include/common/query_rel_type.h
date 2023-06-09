#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class QueryRelType : uint8_t {
    NON_RECURSIVE = 0,
    VARIABLE_LENGTH = 1,
    SHORTEST = 2,
    ALL_SHORTEST = 3,
};

struct QueryRelTypeUtils {
    static inline bool isRecursive(QueryRelType queryRelType) {
        return queryRelType != QueryRelType::NON_RECURSIVE;
    }
};

} // namespace common
} // namespace kuzu
