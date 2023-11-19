#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class RelDataDirection : uint8_t { FWD = 0, BWD = 1 };

struct RelDataDirectionUtils {
    static std::string relDirectionToString(RelDataDirection direction);
};

} // namespace common
} // namespace kuzu
