#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class TableType : uint8_t {
    UNKNOWN = 0,
    NODE = 1,
    REL = 2,
    RDF = 3,
    REL_GROUP = 4,
};

} // namespace common
} // namespace kuzu
