#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorCastOperations {
    // result contains preallocated Value objects with the structured operand's dataType.
    static void castStructuredToUnstructuredValue(ValueVector& operand, ValueVector& result);
    // result contains preallocated gf_string_t objects.
    static void castStructuredToStringValue(ValueVector& operand, ValueVector& result);
    // result contains preallocated Value objects with the structured operand's dataType.
    static void castUnstructuredToBoolValue(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
