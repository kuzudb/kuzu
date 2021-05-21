#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum StatementType : uint8_t {

    /**
     * Reading clause
     **/
    LOAD_CSV_STATEMENT = 0,
    MATCH_STATEMENT = 1,
};

} // namespace common
} // namespace graphflow
