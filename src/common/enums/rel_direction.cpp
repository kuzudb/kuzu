#include "common/enums/rel_direction.h"

#include <array>

#include "common/assert.h"

namespace kuzu {
namespace common {

RelDataDirection RelDirectionUtils::getOppositeDirection(RelDataDirection direction) {
    static constexpr std::array oppositeDirections = {RelDataDirection::BWD, RelDataDirection::FWD};
    return oppositeDirections[relDirectionToKeyIdx(direction)];
}

std::string RelDirectionUtils::relDirectionToString(RelDataDirection direction) {
    static constexpr std::array directionStrs = {"fwd", "bwd"};
    return directionStrs[relDirectionToKeyIdx(direction)];
}

common::idx_t RelDirectionUtils::relDirectionToKeyIdx(RelDataDirection direction) {
    switch (direction) {
    case RelDataDirection::FWD:
        return 0;
    case RelDataDirection::BWD:
        return 1;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
