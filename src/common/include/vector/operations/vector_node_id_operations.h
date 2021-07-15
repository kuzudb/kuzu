#pragma once

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct VectorNodeIDOperations {
    // result = left.nodeIDs == right.nodeIDs
    static void Equals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left.nodeIDs != right.nodeIDs
    static void NotEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left.nodeIDs > right.nodeIDs
    static void GreaterThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left.nodeIDs >= right.nodeIDs
    static void GreaterThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left.nodeIDs < right.nodeIDs
    static void LessThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left.nodeIDs <= right.nodeIDs
    static void LessThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = Hash(operand.nodeIDs)
    static void Hash(ValueVector& operand, ValueVector& result);

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
