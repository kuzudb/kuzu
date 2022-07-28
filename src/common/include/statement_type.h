#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATENODECLAUSE = 1,
    COPYCSV = 2,
};

} // namespace common
} // namespace graphflow
