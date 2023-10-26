#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ColumnDataFormat : uint8_t { REGULAR_COL = 0, CSR_COL = 1 };

}
} // namespace kuzu
