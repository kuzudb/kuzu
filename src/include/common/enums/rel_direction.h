#pragma once

#include <cstdint>
#include <string>

#include "common/types/types.h"

namespace kuzu {
namespace common {

enum class RelDataDirection : uint8_t { FWD = 0, BWD = 1, INVALID = 255 };
enum class RelStorageDirection : uint8_t { FWD_ONLY = 0, BOTH = 1, INVALID = 255 };

struct RelDirectionUtils {
    static RelDataDirection getOppositeDirection(RelDataDirection direction);

    static std::string relDirectionToString(RelDataDirection direction);
    static std::string relStorageDirectionToString(RelStorageDirection direction);
    static common::idx_t relDirectionToKeyIdx(RelDataDirection direction);
    static RelStorageDirection getRelStorageDirection(std::string_view directionStr);

    static constexpr RelStorageDirection DEFAULT_REL_STORAGE_DIRECTION = RelStorageDirection::BOTH;
};

} // namespace common
} // namespace kuzu
