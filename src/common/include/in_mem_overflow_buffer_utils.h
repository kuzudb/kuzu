#pragma once

#include "type_utils.h"

#include "src/common/include/in_mem_overflow_buffer.h"

namespace graphflow {
namespace common {

class InMemOverflowBufferUtils {
public:
    // Currently, this function is only used in `allocateStringIfNecessary`. Make this function
    // private once we remove allocateStringIfNecessary.
    static inline void allocateSpaceForStringIfNecessary(
        gf_string_t& result, uint64_t numBytes, InMemOverflowBuffer& buffer) {
        if (gf_string_t::isShortString(numBytes)) {
            return;
        }
        result.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }

    static void copyString(
        const char* src, uint64_t len, gf_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);
    static void copyString(
        const gf_string_t& src, gf_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);

    static void copyListNonRecursive(const uint8_t* srcValues, gf_list_t& dest,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer);
    static void copyListNonRecursive(const vector<uint8_t*>& srcValues, gf_list_t& dest,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer);
    static void copyListRecursiveIfNested(const gf_list_t& src, gf_list_t& dest,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer,
        uint32_t srcStartIdx = 0, uint32_t srcEndIdx = UINT32_MAX);

    template<typename T>
    static inline void setListElement(gf_list_t& result, uint64_t elementPos, T& element,
        const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
        reinterpret_cast<T*>(result.overflowPtr)[elementPos] = element;
    }

private:
    static inline void allocateSpaceForList(
        gf_list_t& list, uint64_t numBytes, InMemOverflowBuffer& buffer) {
        list.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }
};

template<>
inline void InMemOverflowBufferUtils::setListElement(gf_list_t& result, uint64_t elementPos,
    gf_string_t& element, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    gf_string_t elementToAppend;
    InMemOverflowBufferUtils::copyString(element, elementToAppend, inMemOverflowBuffer);
    reinterpret_cast<gf_string_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

template<>
inline void InMemOverflowBufferUtils::setListElement(gf_list_t& result, uint64_t elementPos,
    gf_list_t& element, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    gf_list_t elementToAppend;
    InMemOverflowBufferUtils::copyListRecursiveIfNested(
        element, elementToAppend, *dataType.childType, inMemOverflowBuffer);
    reinterpret_cast<gf_list_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

} // namespace common
} // namespace graphflow
