#include "include/overflow_buffer_utils.h"

namespace graphflow {
namespace common {

void OverflowBufferUtils::copyString(
    const char* src, uint64_t len, gf_string_t& dest, OverflowBuffer& overflowBuffer) {
    OverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, len, overflowBuffer);
    dest.set(src, len);
}

void OverflowBufferUtils::copyString(
    const gf_string_t& src, gf_string_t& dest, OverflowBuffer& destOverflowBuffer) {
    OverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, src.len, destOverflowBuffer);
    dest.set(src);
}

void OverflowBufferUtils::copyListNonRecursive(const uint8_t* srcValues, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    OverflowBufferUtils::allocateSpaceForList(
        dest, dest.size * Types::getDataTypeSize(*dataType.childType), overflowBuffer);
    dest.set(srcValues, dataType);
}

void OverflowBufferUtils::copyListNonRecursive(const vector<uint8_t*>& srcValues, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    OverflowBufferUtils::allocateSpaceForList(
        dest, srcValues.size() * Types::getDataTypeSize(*dataType.childType), overflowBuffer);
    dest.set(srcValues, dataType.childType->typeID);
}

void OverflowBufferUtils::copyListRecursiveIfNested(const gf_list_t& src, gf_list_t& dest,
    const DataType& dataType, OverflowBuffer& overflowBuffer, uint32_t srcStartIdx,
    uint32_t srcEndIdx) {
    if (srcEndIdx == UINT32_MAX) {
        srcEndIdx = src.size - 1;
    }
    assert(srcEndIdx < src.size);
    auto numElements = srcEndIdx - srcStartIdx + 1;
    auto elementSize = Types::getDataTypeSize(*dataType.childType);
    OverflowBufferUtils::allocateSpaceForList(dest, numElements * elementSize, overflowBuffer);
    memcpy((uint8_t*)dest.overflowPtr, (uint8_t*)src.overflowPtr + srcStartIdx * elementSize,
        numElements * elementSize);
    dest.size = numElements;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < dest.size; i++) {
            OverflowBufferUtils::copyString(((gf_string_t*)src.overflowPtr)[i + srcStartIdx],
                ((gf_string_t*)dest.overflowPtr)[i], overflowBuffer);
        }
    }
    if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < dest.size; i++) {
            OverflowBufferUtils::copyListRecursiveIfNested(
                ((gf_list_t*)src.overflowPtr)[i + srcStartIdx], ((gf_list_t*)dest.overflowPtr)[i],
                *dataType.childType, overflowBuffer);
        }
    }
}

} // namespace common
} // namespace graphflow
