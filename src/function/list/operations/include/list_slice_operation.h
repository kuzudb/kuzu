#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_list.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first element in the
    // list is 1).
    static inline void operation(gf_list_t& list, int64_t& begin, int64_t& end, gf_list_t& result,
        bool isListNull, bool isBeginNull, bool isEndNull, ValueVector& resultValueVector) {
        assert(!isListNull && !isBeginNull && !isEndNull);
        auto elementSize = Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.size = end - begin;
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(result.size * elementSize));
        TypeUtils::copyListRecursiveIfNested(list, result, resultValueVector.dataType,
            resultValueVector.getOverflowBuffer(), begin - 1, end - 2);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
