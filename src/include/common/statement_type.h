#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_NODE_CLAUSE = 1,
    CREATE_REL_CLAUSE = 2,
    COPY_CSV = 3,
    DROP_TABLE = 4,
};

} // namespace common
} // namespace kuzu
