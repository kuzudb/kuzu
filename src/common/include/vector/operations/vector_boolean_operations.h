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
};

} // namespace common
} // namespace graphflow
