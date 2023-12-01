#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ListPrepend {
    template<typename T>
    static inline void operation(common::list_entry_t& listEntry, T& value,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, listEntry.size + 1);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        resultDataVector->copyFromVectorData(
            common::ListVector::getListValues(&resultVector, result), &valueVector,
            reinterpret_cast<uint8_t*>(&value));
        auto resultPos = result.offset + 1;
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto listPos = listEntry.offset;
        for (auto i = 0u; i < listEntry.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, listDataVector, listPos++);
        }
    }
};

} // namespace function
} // namespace kuzu
