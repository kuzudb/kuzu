#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum StatementType : uint8_t {

    /**
     * Reading clause
     **/
    LOAD_CSV = 0,
    MATCH = 1,
};

} // namespace common
} // namespace graphflow
