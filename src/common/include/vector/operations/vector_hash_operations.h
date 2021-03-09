#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorHashOperations {
    // Only support NodeIDVector as operand in this function
    static void HashNodeID(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
