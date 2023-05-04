#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first value in the
    // listEntry is 1).
    static inline void operation(common::list_entry_t& listEntry, int64_t& begin, int64_t& end,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& resultVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? listEntry.size : end;
        result = common::ListVector::addList(&resultVector, endIdx - startIdx);
        auto srcValues =
            common::ListVector::getListValuesWithOffset(&listVector, listEntry, startIdx - 1);
        auto dstValues = common::ListVector::getListValues(&resultVector, result);
        auto numBytesPerValue =
            common::ListVector::getDataVector(&listVector)->getNumBytesPerValue();
        auto srcDataVector = common::ListVector::getDataVector(&listVector);
        auto dstDataVector = common::ListVector::getDataVector(&resultVector);
        for (auto i = startIdx; i < endIdx; i++) {
            common::ValueVectorUtils::copyValue(
                dstValues, *dstDataVector, srcValues, *srcDataVector);
            srcValues += numBytesPerValue;
            dstValues += numBytesPerValue;
        }
    }

    static inline void operation(common::ku_string_t& str, int64_t& begin, int64_t& end,
        common::ku_string_t& result, common::ValueVector& listValueVector,
        common::ValueVector& resultValueVector) {
        int64_t startIdx = (begin == 0) ? 1 : begin;
        int64_t endIdx = (end == 0) ? str.len : end;
        result.len = std::min(endIdx - startIdx + 1, str.len - startIdx + 1);

        if (!common::ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultValueVector)
                    ->allocateSpace(result.len));
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
