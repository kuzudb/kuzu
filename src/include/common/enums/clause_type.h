#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ClauseType : uint8_t {
    // updating clause
    SET = 0,
    DELETE_ = 1, // winnt.h defines DELETE as a macro, so we use DELETE_ instead of DELETE.
    INSERT = 2,
    MERGE = 3,

    // reading clause
    MATCH = 10,
    UNWIND = 11,
    IN_QUERY_CALL = 12,
    LOAD_FROM = 13,
};

enum class MatchClauseType : uint8_t {
    MATCH = 0,
    OPTIONAL_MATCH = 1,
};

enum class DeleteClauseType : uint8_t {
    DELETE = 0,
    DETACH_DELETE = 1,
};

} // namespace common
} // namespace kuzu
