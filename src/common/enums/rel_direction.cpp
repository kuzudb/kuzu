#include "common/enums/rel_direction.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

std::string RelDataDirectionUtils::relDirectionToString(RelDataDirection direction) {
    switch (direction) {
    case RelDataDirection::FWD:
        return "fwd";
    case RelDataDirection::BWD:
        return "bwd";
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
