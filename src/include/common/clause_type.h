#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ClauseType : uint8_t {
    // updating clause
    SET = 0,
    DELETE = 1,
    CREATE = 2,
    MATCH = 3,
    UNWIND = 4
};

} // namespace common
} // namespace kuzu
