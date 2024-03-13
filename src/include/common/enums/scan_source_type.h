#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ScanSourceType : uint8_t {
    EMPTY = 0,
    FILE = 1,
    OBJECT = 2,
    QUERY = 3,
};

}
} // namespace kuzu
