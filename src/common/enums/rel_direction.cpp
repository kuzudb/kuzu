#include "common/enums/rel_direction.h"

#include <array>

#include "common/assert.h"
#include "common/exception/binder.h"

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

std::string RelDirectionUtils::relStorageDirectionToString(RelStorageDirection direction) {
    switch (direction) {
    case RelStorageDirection::FWD_ONLY:
        return "fwd_only";
    case RelStorageDirection::BOTH:
        return "both";
    default:
        KU_UNREACHABLE;
    }
}

RelStorageDirection RelDirectionUtils::getRelStorageDirection(std::string_view directionStr) {
    if (directionStr == "fwd_only") {
        return RelStorageDirection::FWD_ONLY;
    } else if (directionStr == "both") {
        return RelStorageDirection::BOTH;
    } else {
        throw BinderException(
            stringFormat("Cannot bind {} as rel storage direction.", directionStr));
    }
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
