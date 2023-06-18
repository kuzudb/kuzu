#pragma once

#include <set>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

template<typename T>
struct ListUnique {
    static inline void operation(common::list_entry_t& input, int64_t& result,
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
        result = uniqueValues.size();
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
