#include "common/enums/rel_direction.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

std::string RelDataDirectionUtils::relDirectionToString(RelDataDirection direction) {
    switch (direction) {
    case RelDataDirection::FWD:
        return "forward";
    case RelDataDirection::BWD:
        return "backward";
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
