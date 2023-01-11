#pragma once

#include "common/in_mem_overflow_buffer.h"
#include "type_utils.h"

namespace kuzu {
namespace common {

class InMemOverflowBufferUtils {
public:
    // Currently, this function is only used in `allocateStringIfNecessary`. Make this function
    // private once we remove allocateStringIfNecessary.
    static inline void allocateSpaceForStringIfNecessary(
        ku_string_t& result, uint64_t numBytes, InMemOverflowBuffer& buffer) {
        if (ku_string_t::isShortString(numBytes)) {
            return;
        }
        result.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }

    static void copyString(
        const char* src, uint64_t len, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);
    static void copyString(
        const ku_string_t& src, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);

    static void copyListNonRecursive(const uint8_t* srcValues, ku_list_t& dest,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer);

    static void copyListRecursiveIfNested(const ku_list_t& src, ku_list_t& dest,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer,
        uint32_t srcStartIdx = 0, uint32_t srcEndIdx = UINT32_MAX);

    template<typename T>
    static inline void setListElement(ku_list_t& result, uint64_t elementPos, T& element,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
        reinterpret_cast<T*>(result.overflowPtr)[elementPos] = element;
    }

    static inline void allocateSpaceForList(
        ku_list_t& list, uint64_t numBytes, InMemOverflowBuffer& buffer) {
        list.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }
};

template<>
inline void InMemOverflowBufferUtils::setListElement(ku_list_t& result, uint64_t elementPos,
    ku_string_t& element, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    ku_string_t elementToAppend;
    InMemOverflowBufferUtils::copyString(element, elementToAppend, inMemOverflowBuffer);
    reinterpret_cast<ku_string_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

template<>
inline void InMemOverflowBufferUtils::setListElement(ku_list_t& result, uint64_t elementPos,
    ku_list_t& element, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    ku_list_t elementToAppend;
    InMemOverflowBufferUtils::copyListRecursiveIfNested(
        element, elementToAppend, *dataType.childType, inMemOverflowBuffer);
    reinterpret_cast<ku_list_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

} // namespace common
} // namespace kuzu
