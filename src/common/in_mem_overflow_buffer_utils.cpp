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

void InMemOverflowBufferUtils::copyListRecursiveIfNested(const ku_list_t& src, ku_list_t& dst,
    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer, uint32_t srcStartIdx,
    uint32_t srcEndIdx) {
    if (src.size == 0) {
        dst.size = 0;
        return;
    }
    if (srcEndIdx == UINT32_MAX) {
        srcEndIdx = src.size - 1;
    }
    assert(srcEndIdx < src.size);
    auto numElements = srcEndIdx - srcStartIdx + 1;
    auto elementSize = Types::getDataTypeSize(*dataType.getChildType());
    InMemOverflowBufferUtils::allocateSpaceForList(
        dst, numElements * elementSize, inMemOverflowBuffer);
    memcpy((uint8_t*)dst.overflowPtr, (uint8_t*)src.overflowPtr + srcStartIdx * elementSize,
        numElements * elementSize);
    dst.size = numElements;
    if (dataType.getChildType()->typeID == STRING) {
        for (auto i = 0u; i < dst.size; i++) {
            InMemOverflowBufferUtils::copyString(((ku_string_t*)src.overflowPtr)[i + srcStartIdx],
                ((ku_string_t*)dst.overflowPtr)[i], inMemOverflowBuffer);
        }
    }
    if (dataType.getChildType()->typeID == VAR_LIST) {
        for (auto i = 0u; i < dst.size; i++) {
            InMemOverflowBufferUtils::copyListRecursiveIfNested(
                ((ku_list_t*)src.overflowPtr)[i + srcStartIdx], ((ku_list_t*)dst.overflowPtr)[i],
                *dataType.getChildType(), inMemOverflowBuffer);
        }
    }
}

} // namespace common
} // namespace kuzu
