#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class ColumnEvaluateType : uint8_t {
    REFERENCE = 0,
    DEFAULT = 1,
    CAST = 2,
};

}
} // namespace kuzu
