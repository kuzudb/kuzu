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
    // result = abs(operand)
    static void Abs(ValueVector& operand, ValueVector& result);
    // result = floor(operand)
    static void Floor(ValueVector& operand, ValueVector& result);
    // result = ceil(operand)
    static void Ceil(ValueVector& operand, ValueVector& result);
    // result = interval(operand)
    static void Interval(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
