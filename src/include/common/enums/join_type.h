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

} // namespace common
} // namespace kuzu
