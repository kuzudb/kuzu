#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class JoinType : uint8_t {
    INNER = 0,
    LEFT = 1,
    MARK = 2,
    COUNT = 3,
};

enum class AccumulateType : uint8_t {
    REGULAR = 0,
    OPTIONAL_ = 1,
};

} // namespace common
} // namespace kuzu
