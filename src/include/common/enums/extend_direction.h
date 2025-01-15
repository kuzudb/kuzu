#pragma once

#include <cstdint>

#include "common/assert.h"
#include "rel_direction.h"

namespace kuzu {
namespace common {

enum class ExtendDirection : uint8_t { FWD = 0, BWD = 1, BOTH = 2 };

class ExtendDirectionUtil {
public:
    static RelDataDirection getRelDataDirection(ExtendDirection direction) {
        KU_ASSERT(direction != ExtendDirection::BOTH);
        return direction == ExtendDirection::FWD ? RelDataDirection::FWD : RelDataDirection::BWD;
    }

    static ExtendDirection fromString(const std::string& str);
    static std::string toString(ExtendDirection direction);

    static ExtendDirection getDefaultExtendDirection();
};

} // namespace common
} // namespace kuzu
