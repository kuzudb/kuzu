#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ListReverse {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        result = input; // reverse does not change
        for (auto i = 0u; i < input.size; i++) {
            auto pos = input.offset + i;
            auto reversePos = input.offset + input.size - 1 - i;
            resultDataVector->copyFromVectorData(reversePos, inputDataVector, pos);
        }
    }
};

} // namespace function
} // namespace kuzu
