#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorCastOperations {
    // result contains preallocated Value objects with the structured operand's dataType.
    static void castStructuredToUnknownValue(ValueVector& operand, ValueVector& result);
    // result contains preallocated Value objects with the structured operand's dataType.
    static void castUnknownToBoolValue(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
