#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorNullOperations {
    // result = operand == NULL
    static void IsNull(ValueVector& operand, ValueVector& result);
    // result = operand != NULL
    static void IsNotNull(ValueVector& operand, ValueVector& result);

    static uint64_t IsNullSelect(ValueVector& operand, sel_t* selectedPositions);
    static uint64_t IsNotNullSelect(ValueVector& operand, sel_t* selectedPositions);
};

} // namespace common
} // namespace graphflow
