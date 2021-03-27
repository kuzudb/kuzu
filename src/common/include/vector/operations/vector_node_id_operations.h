#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorNodeIDCompareOperations {
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
};

struct VectorNodeIDOperations {
    static void Hash(ValueVector& operand, ValueVector& result);
    static void Decompress(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
