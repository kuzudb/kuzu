#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class ConflictAction : uint8_t {
    ON_CONFLICT_THROW = 0,
    ON_CONFLICT_DO_NOTHING = 1,
};

struct ConflictActionUtil {
    static std::string toString(ConflictAction conflictAction);
};

} // namespace common
} // namespace kuzu
