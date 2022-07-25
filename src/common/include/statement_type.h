#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CreateNodeClause = 1,
};

} // namespace common
} // namespace graphflow
