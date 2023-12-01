#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ListAppend {
    template<typename T>
    static inline void operation(common::list_entry_t& listEntry, T& value,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, listEntry.size + 1);
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto listPos = listEntry.offset;
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultPos = result.offset;
        for (auto i = 0u; i < listEntry.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, listDataVector, listPos++);
        }
        resultDataVector->copyFromVectorData(
            resultDataVector->getData() + resultPos * resultDataVector->getNumBytesPerValue(),
            &valueVector, reinterpret_cast<uint8_t*>(&value));
    }
};

} // namespace function
} // namespace kuzu
