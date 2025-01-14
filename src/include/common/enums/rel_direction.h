#pragma once

#include <cstdint>
#include <string>

#include "common/types/types.h"

namespace kuzu {
namespace common {

enum class RelDataDirection : uint8_t { FWD = 0, BWD = 1, INVALID = 255 };
static constexpr idx_t NUM_REL_DIRECTIONS = 2;

struct RelDirectionUtils {
    static RelDataDirection getOppositeDirection(RelDataDirection direction);

    static std::string relDirectionToString(RelDataDirection direction);
    static common::idx_t relDirectionToKeyIdx(RelDataDirection direction);
};

} // namespace common
} // namespace kuzu
