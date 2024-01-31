#pragma once

#include "function/string/functions/substr_function.h"

namespace kuzu {
namespace function {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first value in the
    // listEntry is 1).
    static inline void operation(common::list_entry_t& listEntry, int64_t& begin, int64_t& end,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& resultVector) {
        auto startIdx = begin;
        auto endIdx = end;
        normalizeIndices(startIdx, endIdx, listEntry.size);
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
        common::ku_string_t& result, common::ValueVector& /*listValueVector*/,
        common::ValueVector& resultValueVector) {
        auto startIdx = begin;
        auto endIdx = end;
        normalizeIndices(startIdx, endIdx, str.len);
        SubStr::operation(str, startIdx, std::min(endIdx - startIdx + 1, str.len - startIdx + 1),
            result, resultValueVector);
    }

private:
    static inline void normalizeIndices(int64_t& startIdx, int64_t& endIdx, uint64_t size) {
        if (startIdx <= 0) {
            startIdx = 1;
        }
        if (endIdx <= 0 || (uint64_t)endIdx > size) {
            endIdx = size + 1;
        }
        if (startIdx > endIdx) {
            endIdx = startIdx;
        }
    }
};

} // namespace function
} // namespace kuzu
