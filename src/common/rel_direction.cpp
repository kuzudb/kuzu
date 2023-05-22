#include "common/rel_direction.h"

#include "common/exception.h"

namespace kuzu {
namespace common {

std::string RelDataDirectionUtils::relDataDirectionToString(RelDataDirection direction) {
    switch (direction) {
    case RelDataDirection::FWD: {
        return "forward";
    }
    case RelDataDirection::BWD: {
        return "backward";
    }
    default:
        throw NotImplementedException("RelDataDirectionUtils::relDataDirectionToString");
    }
}

} // namespace common
} // namespace kuzu
