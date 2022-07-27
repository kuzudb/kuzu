#pragma once

#include "src/common/include/overflow_buffer_utils.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

class ValueVectorUtils {

public:
    static void addLiteralToStructuredVector(
        ValueVector& resultVector, uint64_t pos, const Literal& literal);

    template<typename T>
    static inline void templateCopyValue(
        T* src, T* dest, const DataType& dataType, OverflowBuffer& overflowBuffer) {
        if constexpr (is_same<T, gf_string_t>::value) {
            OverflowBufferUtils::copyString(const_cast<const T&>(*src), *dest, overflowBuffer);
        } else if constexpr (is_same<T, gf_list_t>::value) {
            OverflowBufferUtils::copyListRecursiveIfNested(
                const_cast<const T&>(*src), *dest, dataType, overflowBuffer);
        } else if constexpr (is_same<T, Value>::value) {
            memcpy(dest, src, sizeof(T));
            if (src->dataType.typeID == STRING) {
                OverflowBufferUtils::copyString(src->val.strVal, dest->val.strVal, overflowBuffer);
            }
        } else {
            memcpy(dest, src, sizeof(T));
        }
    }

    static void copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector, uint64_t pos,
        uint8_t* dstData, OverflowBuffer& dstOverflowBuffer);

private:
    static void copyNonNullDataWithSameType(const DataType& dataType, const uint8_t* srcData,
        uint8_t* dstData, OverflowBuffer& overflowBuffer);
};

} // namespace common
} // namespace graphflow
