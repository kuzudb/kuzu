#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class DropType : uint8_t {
    TABLE = 0,
    SEQUENCE = 1,
};

struct DropTypeUtils {
    static std::string toString(DropType type);
};

} // namespace common
} // namespace kuzu
