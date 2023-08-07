#pragma once

#include <cstddef>

namespace kuzu {
namespace planner {

enum class SidewaysInfoPassing : uint8_t {
    NONE = 0,
    PROBE_TO_BUILD = 1,
    PROHIBIT_PROBE_TO_BUILD = 2,
    BUILD_TO_PROBE = 3,
    PROHIBIT_BUILD_TO_PROBE = 4,
};

} // namespace planner
} // namespace kuzu
