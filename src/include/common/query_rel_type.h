#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class QueryRelType : uint8_t {
    NON_RECURSIVE = 0,
    VARIABLE_LENGTH = 1,
    SHORTEST = 2,
};

} // namespace common
} // namespace kuzu
