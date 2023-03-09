#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"
#include "common/types/ku_string.h"
#include "function/string/operations/array_extract_operation.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListExtract {
public:
    template<typename T>
    static inline void setValue(T& src, T& dest, common::ValueVector& resultValueVector) {
        dest = src;
    }

    // Note: this function takes in a 1-based position (The index of the first element in the list
    // is 1).
    template<typename T>
    static inline void operation(
        common::ku_list_t& list, int64_t pos, T& result, common::ValueVector& resultValueVector) {
        auto uint64Pos = (uint64_t)pos;
        if (list.size < uint64Pos) {
            throw common::RuntimeException("list_extract(list, index): index=" +
                                           common::TypeUtils::toString(pos) + " is out of range.");
        }
        auto values = reinterpret_cast<T*>(list.overflowPtr);
        result = values[uint64Pos - 1];
        setValue(values[uint64Pos - 1], result, resultValueVector);
    }

    static inline void operation(
        common::ku_string_t& str, int64_t& idx, common::ku_string_t& result) {
        if (str.len < idx) {
            result.set("", 0);
        } else {
            ArrayExtract::operation(str, idx, result);
        }
    }
};

template<>
inline void ListExtract::setValue(
    common::ku_string_t& src, common::ku_string_t& dest, common::ValueVector& resultValueVector) {
    if (!common::ku_string_t::isShortString(src.len)) {
        dest.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(src.len));
    }
    dest.set(src);
}

template<>
inline void ListExtract::setValue(
    common::ku_list_t& src, common::ku_list_t& dest, common::ValueVector& resultValueVector) {
    common::InMemOverflowBufferUtils::copyListRecursiveIfNested(
        src, dest, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
}

} // namespace operation
} // namespace function
} // namespace kuzu
