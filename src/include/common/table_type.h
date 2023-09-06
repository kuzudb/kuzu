#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class TableType : uint8_t {
    NODE = 0,
    REL = 1,
    RDF = 2,
};

} // namespace common
} // namespace kuzu
