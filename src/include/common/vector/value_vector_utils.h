#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

class ValueVectorUtils {
public:
    // These two functions assume that the given uint8_t* srcData/dstData are pointing to a data
    // with the same data type as this ValueVector.
    static void copyNonNullDataWithSameTypeIntoPos(
        ValueVector& resultVector, uint64_t pos, const uint8_t* srcData);
    static void copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector, uint64_t pos,
        uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer);
    static void copyValue(uint8_t* dstValue, common::ValueVector& dstVector,
        const uint8_t* srcValue, const common::ValueVector& srcVector);
};

} // namespace common
} // namespace kuzu
