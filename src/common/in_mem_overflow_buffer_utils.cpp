#include "common/in_mem_overflow_buffer_utils.h"

namespace kuzu {
namespace common {

void InMemOverflowBufferUtils::copyString(
    const char* src, uint64_t len, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, len, inMemOverflowBuffer);
    dest.set(src, len);
}

void InMemOverflowBufferUtils::copyString(
    const ku_string_t& src, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, src.len, inMemOverflowBuffer);
    dest.set(src);
}

void InMemOverflowBufferUtils::copyListNonRecursive(const uint8_t* srcValues, ku_list_t& dest,
    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForList(
        dest, dest.size * Types::getDataTypeSize(*dataType.childType), inMemOverflowBuffer);
    dest.set(srcValues, dataType);
}

void InMemOverflowBufferUtils::copyListRecursiveIfNested(const ku_list_t& src, ku_list_t& dest,
    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer, uint32_t srcStartIdx,
    uint32_t srcEndIdx) {
    if (src.size == 0) {
        dest.size = 0;
        return;
    }
    if (srcEndIdx == UINT32_MAX) {
        srcEndIdx = src.size - 1;
    }
    assert(srcEndIdx < src.size);
    auto numElements = srcEndIdx - srcStartIdx + 1;
    auto elementSize = Types::getDataTypeSize(*dataType.childType);
    InMemOverflowBufferUtils::allocateSpaceForList(
        dest, numElements * elementSize, inMemOverflowBuffer);
    memcpy((uint8_t*)dest.overflowPtr, (uint8_t*)src.overflowPtr + srcStartIdx * elementSize,
        numElements * elementSize);
    dest.size = numElements;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < dest.size; i++) {
            InMemOverflowBufferUtils::copyString(((ku_string_t*)src.overflowPtr)[i + srcStartIdx],
                ((ku_string_t*)dest.overflowPtr)[i], inMemOverflowBuffer);
        }
    }
    if (dataType.childType->typeID == VAR_LIST) {
        for (auto i = 0u; i < dest.size; i++) {
            InMemOverflowBufferUtils::copyListRecursiveIfNested(
                ((ku_list_t*)src.overflowPtr)[i + srcStartIdx], ((ku_list_t*)dest.overflowPtr)[i],
                *dataType.childType, inMemOverflowBuffer);
        }
    }
}

} // namespace common
} // namespace kuzu
