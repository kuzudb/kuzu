#pragma once

#include <cstdint>

namespace kuzu {
namespace planner {

enum class SidewaysInfoPassing : uint8_t {
    NONE = 0,
    PROHIBIT = 1,
    PROBE_TO_BUILD = 2,
    PROHIBIT_PROBE_TO_BUILD = 3,
    BUILD_TO_PROBE = 4,
    PROHIBIT_BUILD_TO_PROBE = 5,
};

} // namespace planner
} // namespace kuzu
