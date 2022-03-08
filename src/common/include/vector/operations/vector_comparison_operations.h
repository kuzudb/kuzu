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

    static uint64_t EqualsSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t NotEqualsSelect(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t GreaterThanSelect(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t GreaterThanEqualsSelect(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t LessThanSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);
    static uint64_t LessThanEqualsSelect(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions);
};

} // namespace common
} // namespace graphflow
