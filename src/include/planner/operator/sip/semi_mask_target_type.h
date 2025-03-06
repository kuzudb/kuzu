#pragma once

#include <cstdint>

namespace kuzu {
namespace planner {

enum class SemiMaskTargetType : uint8_t {
    SCAN_NODE = 0,
    GDS_INPUT_NODE = 2,
    GDS_OUTPUT_NODE = 3,
    // Used for recursive join.
    GDS_PATH_NODE = 4,
    // Used for GDS algorithms.
    GDS_GRAPH_NODE = 5,
};

}
} // namespace kuzu
