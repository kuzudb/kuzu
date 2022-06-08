#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_list.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
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
        gf_list_t& list, int64_t pos, T& result, ValueVector& resultValueVector) {
        auto uint64Pos = (uint64_t)pos;
        if (list.size < uint64Pos) {
            throw RuntimeException("list_extract(list, index): index=" + TypeUtils::toString(pos) +
                                   " is out of range.");
        }
        auto values = reinterpret_cast<T*>(list.overflowPtr);
        result = values[uint64Pos - 1];
        setValue(values[uint64Pos - 1], result, resultValueVector);
    }
};

template<>
inline void ListExtract::setValue(
    gf_string_t& src, gf_string_t& dest, ValueVector& resultValueVector) {
    if (!gf_string_t::isShortString(src.len)) {
        dest.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(src.len));
    }
    dest.set(src);
}

template<>
inline void ListExtract::setValue(gf_list_t& src, gf_list_t& dest, ValueVector& resultValueVector) {
    OverflowBufferUtils::copyListRecursiveIfNested(
        src, dest, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
}

} // namespace operation
} // namespace function
} // namespace graphflow
