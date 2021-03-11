#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorComparisonOperations {
    // result = left == right
    static void Equals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left != right
    static void NotEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left > right
    static void GreaterThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left >= right
    static void GreaterThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left < right
    static void LessThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left <= right
    static void LessThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);
};

} // namespace common
} // namespace graphflow
