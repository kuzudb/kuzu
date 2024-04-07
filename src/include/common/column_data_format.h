#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ColumnDataFormat : uint8_t { REGULAR = 0, CSR = 1 };

} // namespace common
} // namespace kuzu
