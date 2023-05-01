#pragma once

#include <cassert>
#include <cstring>

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/types/ku_list.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListAppend {
    template<typename T>
    static inline void operation(common::list_entry_t& listEntry, T& value,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, listEntry.size + 1);
        auto listValues = common::ListVector::getListValues(&listVector, listEntry);
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto resultValues = common::ListVector::getListValues(&resultVector, result);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
        for (auto i = 0u; i < listEntry.size; i++) {
            common::ValueVectorUtils::copyValue(
                resultValues, *resultDataVector, listValues, *listDataVector);
            listValues += numBytesPerValue;
            resultValues += numBytesPerValue;
        }
        common::ValueVectorUtils::copyValue(
            resultValues, *resultDataVector, reinterpret_cast<uint8_t*>(&value), valueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
