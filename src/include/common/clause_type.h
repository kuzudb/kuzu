#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ClauseType : uint8_t {
    // updating clause
    SET = 0,
    // winnt.h defines DELETE as a macro, so we need to use DELETE_ instead of DELETE.
    DELETE_ = 1,
    CREATE = 2,
    // reading clause
    MATCH = 3,
    UNWIND = 4
};

} // namespace common
} // namespace kuzu
