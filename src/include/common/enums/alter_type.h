#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class AlterType : uint8_t {
    RENAME_TABLE = 0,

    ADD_PROPERTY = 10,
    DROP_PROPERTY = 11,
    RENAME_PROPERTY = 12,

    SET_COMMENT = 20,
};

enum class CommentType : uint8_t {
    TABLE_ENTRY = 0,
    SCALAR_MACRO_ENTRY = 1,
};

} // namespace common
} // namespace kuzu
