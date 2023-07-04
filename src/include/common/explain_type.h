#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ExplainType : uint8_t {
    PROFILE = 0,
    PHYSICAL_PLAN = 1,
};

} // namespace common
} // namespace kuzu
