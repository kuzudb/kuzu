#pragma once

#include <cassert>
#include <cstring>

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
            resultDataVector->copyFromVectorData(resultValues, listDataVector, listValues);
            listValues += numBytesPerValue;
            resultValues += numBytesPerValue;
        }
        resultDataVector->copyFromVectorData(
            resultValues, &valueVector, reinterpret_cast<uint8_t*>(&value));
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
