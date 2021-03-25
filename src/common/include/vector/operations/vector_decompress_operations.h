#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorDecompressOperations {
    // Only support NodeIDVector as operand in this function
    static void DecompressNodeID(ValueVector& operand, ValueVector& result);
};
} // namespace common
} // namespace graphflow
