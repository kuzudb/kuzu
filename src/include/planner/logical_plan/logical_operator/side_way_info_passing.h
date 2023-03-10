#pragma once

#include <cstddef>

namespace kuzu {
namespace planner {

enum class SidewaysInfoPassing : uint8_t {
    NONE = 0,
    LEFT_TO_RIGHT = 1,
};

} // namespace planner
} // namespace kuzu
