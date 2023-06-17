#pragma once

#include <set>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

template<typename T>
struct ListDistinct {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        std::set<T> uniqueValues;
        auto inputValues =
            reinterpret_cast<T*>(common::ListVector::getListValues(&inputVector, input));
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);

        for (auto i = 0; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                continue;
            }
            uniqueValues.insert(inputValues[i]);
        }

        result = common::ListVector::addList(&resultVector, uniqueValues.size());
        auto resultValues = common::ListVector::getListValues(&resultVector, result);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto numBytesPerValue = inputDataVector->getNumBytesPerValue();
        for (auto val : uniqueValues) {
            resultDataVector->copyFromVectorData(
                resultValues, inputDataVector, reinterpret_cast<uint8_t*>(&val));
            resultValues += numBytesPerValue;
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
