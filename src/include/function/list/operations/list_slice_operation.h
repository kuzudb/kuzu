#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first element in the
    // list is 1).
    static inline void operation(common::ku_list_t& list, int64_t& begin, int64_t& end,
        common::ku_list_t& result, common::ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? list.size : end;
        auto elementSize =
            common::Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.size = endIdx - startIdx;
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(result.size * elementSize));
        common::InMemOverflowBufferUtils::copyListRecursiveIfNested(list, result,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer(), startIdx - 1,
            endIdx - 2);
    }

    static inline void operation(common::ku_string_t& str, int64_t& begin, int64_t& end,
        common::ku_string_t& result, common::ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? str.len : end;
        result.len = std::min(endIdx - startIdx + 1, str.len - startIdx + 1);

        if (!common::ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        memcpy((uint8_t*)result.getData(), str.getData() + startIdx - 1, result.len);
        if (!common::ku_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), common::ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
