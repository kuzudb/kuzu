#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class AlterType : uint8_t {
    RENAME_TABLE = 0,

    ADD_PROPERTY = 10,
    DROP_PROPERTY = 11,
    RENAME_PROPERTY = 12,
    COMMENT = 201,
    INVALID = 255
};

} // namespace common
} // namespace kuzu
