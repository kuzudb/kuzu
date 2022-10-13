#include "include/in_mem_overflow_buffer_utils.h"

namespace graphflow {
namespace common {

void InMemOverflowBufferUtils::copyString(
    const char* src, uint64_t len, gf_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, len, inMemOverflowBuffer);
    dest.set(src, len);
}

void InMemOverflowBufferUtils::copyString(
    const gf_string_t& src, gf_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, src.len, inMemOverflowBuffer);
    dest.set(src);
}

//void InMemOverflowBufferUtils::copyListNonRecursive(const uint8_t* srcValues, gf_list_t& dest,
//    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
//    InMemOverflowBufferUtils::allocateSpaceForList(
//        dest, dest.size * Types::getDataTypeSize(*dataType.childType), inMemOverflowBuffer);
//    dest.set(srcValues, dataType);
//}

//void InMemOverflowBufferUtils::copyListNonRecursive(const vector<uint8_t*>& srcValues,
//    gf_list_t& dest, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
//    InMemOverflowBufferUtils::allocateSpaceForList(
//        dest, srcValues.size() * Types::getDataTypeSize(*dataType.childType), inMemOverflowBuffer);
//    dest.set(srcValues, dataType.childType->typeID);
//}

void InMemOverflowBufferUtils::copyListRecursiveIfNested(const gf_list_t& src, gf_list_t& dest,
    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer, uint32_t srcStartIdx,
    uint32_t srcEndIdx) {
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
            InMemOverflowBufferUtils::copyString(((gf_string_t*)src.overflowPtr)[i + srcStartIdx],
                ((gf_string_t*)dest.overflowPtr)[i], inMemOverflowBuffer);
        }
    }
    if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < dest.size; i++) {
            InMemOverflowBufferUtils::copyListRecursiveIfNested(
                ((gf_list_t*)src.overflowPtr)[i + srcStartIdx], ((gf_list_t*)dest.overflowPtr)[i],
                *dataType.childType, inMemOverflowBuffer);
        }
    }
}

} // namespace common
} // namespace graphflow
