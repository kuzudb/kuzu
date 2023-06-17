#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

class ValueVectorUtils {
public:
    static void copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector, uint64_t pos,
        uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer);
};

} // namespace common
} // namespace kuzu
