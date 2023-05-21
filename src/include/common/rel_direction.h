#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

enum RelDataDirection : uint8_t { FWD = 0, BWD = 1 };

struct RelDataDirectionUtils {
    static inline std::vector<RelDataDirection> getRelDataDirections() {
        return std::vector<RelDataDirection>{RelDataDirection::FWD, RelDataDirection::BWD};
    }

    static std::string relDataDirectionToString(RelDataDirection direction);
};

} // namespace common
} // namespace kuzu
