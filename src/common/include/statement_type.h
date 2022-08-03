#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_NODE_CLAUSE = 1,
    CREATE_REL_CLAUSE = 2,
    COPY_CSV = 3,
};

} // namespace common
} // namespace graphflow
