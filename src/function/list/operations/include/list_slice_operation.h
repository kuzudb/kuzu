#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/ku_list.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first element in the
    // list is 1).
    static inline void operation(ku_list_t& list, int64_t& begin, int64_t& end, ku_list_t& result,
        ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? list.size : end;
        auto elementSize = Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.size = endIdx - startIdx;
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace(result.size * elementSize));
        InMemOverflowBufferUtils::copyListRecursiveIfNested(list, result,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer(), startIdx - 1,
            endIdx - 2);
    }

    static inline void operation(ku_string_t& str, int64_t& begin, int64_t& end,
        ku_string_t& result, ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? str.len : end;
        result.len = min(endIdx - startIdx + 1, str.len - startIdx + 1);

        if (!ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        memcpy((uint8_t*)result.getData(), str.getData() + startIdx - 1, result.len);
        if (!ku_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), ku_string_t::PREFIX_LENGTH);
        }
    }

    static inline void operation(
        Value& item, int64_t& begin, int64_t& end, Value& result, ValueVector& resultValueVector) {
        if (item.dataType.typeID == STRING) {
            result.dataType.typeID = STRING;
            operation(item.val.strVal, begin, end, result.val.strVal, resultValueVector);
        } else if (item.dataType.typeID == LIST) {
            throw RuntimeException("list_slice not implemented for unstructured lists");
        } else {
            throw RuntimeException(
                "incorrect type given to [] operator. Type must be either LIST or STRING");
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
