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
    UNWIND = 4,
    InQueryCall = 5,
};

enum class MatchClauseType : uint8_t {
    MATCH = 0,
    OPTIONAL_MATCH = 1,
};

} // namespace common
} // namespace kuzu
