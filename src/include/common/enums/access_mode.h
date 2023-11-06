#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class AccessMode : uint8_t { READ_ONLY = 0, READ_WRITE = 1 };

}
} // namespace kuzu
