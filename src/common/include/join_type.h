#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum class JoinType : uint8_t {
    INNER = 0,
    LEFT = 1,
};

} // namespace common
} // namespace graphflow
