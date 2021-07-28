#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorBooleanOperations {
    // result = left && right
    static void And(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left || right
    static void Or(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left ^ right
    static void Xor(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = !vector
    static void Not(ValueVector& operand, ValueVector& result);

    static uint64_t AndSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t OrSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t XorSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t NotSelect(ValueVector& operand, sel_t* selectedPositions);
};

} // namespace common
} // namespace graphflow
