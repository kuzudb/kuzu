#pragma once

#include <cstdint>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

struct DecompressNodeID {
    static inline nodeID_t operation(nodeID_t nodeID) { return nodeID; }
};

} // namespace operation
} // namespace common
} // namespace graphflow
