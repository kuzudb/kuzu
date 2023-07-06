#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first value in the
    // listEntry is 1).
    static inline void operation(common::list_entry_t& listEntry, int64_t& begin, int64_t& end,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& resultVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? listEntry.size : end;
        result = common::ListVector::addList(&resultVector, endIdx - startIdx);
        auto srcDataVector = common::ListVector::getDataVector(&listVector);
        auto srcPos = listEntry.offset + startIdx - 1;
        auto dstDataVector = common::ListVector::getDataVector(&resultVector);
        auto dstPos = result.offset;
        for (auto i = 0u; i < endIdx - startIdx; i++) {
            dstDataVector->copyFromVectorData(dstPos++, srcDataVector, srcPos++);
        }
    }

    static inline void operation(common::ku_string_t& str, int64_t& begin, int64_t& end,
        common::ku_string_t& result, common::ValueVector& listValueVector,
        common::ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? str.len : end;
        result.len = std::min(endIdx - startIdx + 1, str.len - startIdx + 1);
        common::StringVector::addString(
            &resultValueVector, result, (const char*)(str.getData() + startIdx - 1), result.len);
    }
};

} // namespace function
} // namespace kuzu
