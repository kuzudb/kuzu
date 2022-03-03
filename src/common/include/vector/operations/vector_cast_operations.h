#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

// TODO: open an issue to add value level cast operation
struct VectorCastOperations {
    // result contains preallocated Value objects with the structured operand's dataType.
    static void castStructuredToUnstructuredValue(ValueVector& operand, ValueVector& result);
    // result contains preallocated gf_string_t objects.
    static void castStructuredToString(ValueVector& operand, ValueVector& result);
    // VALUE -> VALUE
    static void castUnstructuredToBoolValue(ValueVector& operand, ValueVector& result);
    // STRING -> INTERVAL
    static void castStringToDate(ValueVector& operand, ValueVector& result);
    static void castStringToTimestamp(ValueVector& operand, ValueVector& result);
    static void castStringToInterval(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
