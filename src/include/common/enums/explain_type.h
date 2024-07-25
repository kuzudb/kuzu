#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ExplainType : uint8_t {
    PROFILE = 0,
    PHYSICAL_PLAN = 1,
    LOGICAL_PLAN = 2
};

} // namespace common
} // namespace kuzu
