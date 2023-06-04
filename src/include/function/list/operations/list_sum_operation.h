#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListSum {
    template<typename T>
    static inline void operation(common::list_entry_t& input, T& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto inputValues =
            reinterpret_cast<T*>(common::ListVector::getListValues(&inputVector, input));
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        result = 0;
        for (auto i = 0; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                continue;
            }
            result += inputValues[i];
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
