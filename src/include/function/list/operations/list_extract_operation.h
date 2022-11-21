#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"
#include "common/types/ku_string.h"
#include "function/string/operations/array_extract_operation.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListExtract {
public:
    template<typename T>
    static inline void setValue(T& src, T& dest, ValueVector& resultValueVector) {
        dest = src;
    }

    // Note: this function takes in a 1-based position (The index of the first element in the list
    // is 1).
    template<typename T>
    static inline void operation(
        ku_list_t& list, int64_t pos, T& result, ValueVector& resultValueVector) {
        auto uint64Pos = (uint64_t)pos;
        if (list.size < uint64Pos) {
            throw RuntimeException("list_extract(list, index): index=" + TypeUtils::toString(pos) +
                                   " is out of range.");
        }
        auto values = reinterpret_cast<T*>(list.overflowPtr);
        result = values[uint64Pos - 1];
        setValue(values[uint64Pos - 1], result, resultValueVector);
    }

    static inline void operation(ku_string_t& str, int64_t& idx, ku_string_t& result) {
        if (str.len < idx) {
            result.set("", 0);
        } else {
            ArrayExtract::operation(str, idx, result);
        }
    }
};

template<>
inline void ListExtract::setValue(
    ku_string_t& src, ku_string_t& dest, ValueVector& resultValueVector) {
    if (!ku_string_t::isShortString(src.len)) {
        dest.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(src.len));
    }
    dest.set(src);
}

template<>
inline void ListExtract::setValue(ku_list_t& src, ku_list_t& dest, ValueVector& resultValueVector) {
    InMemOverflowBufferUtils::copyListRecursiveIfNested(
        src, dest, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
}

} // namespace operation
} // namespace function
} // namespace kuzu
