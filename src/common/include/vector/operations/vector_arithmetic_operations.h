#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorArithmeticOperations {
    // result = left + right
    static void Add(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left - right
    static void Subtract(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left * right
    static void Multiply(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left / right
    static void Divide(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left % right
    static void Modulo(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left ^ right
    static void Power(ValueVector& left, ValueVector& right, ValueVector& result);
    // for pos, element in vector: result[pos] = -element
    static void Negate(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
