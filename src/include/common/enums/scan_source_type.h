#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class ScanSourceType : uint8_t {
    EMPTY = 0,
    FILE = 1,
    OBJECT = 2,
    QUERY = 3,
    TABLE_FUNC = 4,
};

class ScanSourceTypeUtils {
public:
    static std::string toString(ScanSourceType type);
};

} // namespace common
} // namespace kuzu
