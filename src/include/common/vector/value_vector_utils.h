#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

class ValueVectorUtils {
public:
    static void copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector, uint64_t pos,
        uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer);
    static void copyValue(uint8_t* dstValue, common::ValueVector& dstVector,
        const uint8_t* srcValue, const common::ValueVector& srcVector);
};

} // namespace common
} // namespace kuzu
