#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ListProduct {
    template<typename T>
    static inline void operation(common::list_entry_t& input, T& result,
        common::ValueVector& inputVector, common::ValueVector& /*resultVector*/) {
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        result = 1;
        for (auto i = 0; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                continue;
            }
            result *= inputDataVector->getValue<T>(input.offset + i);
        }
    }
};

} // namespace function
} // namespace kuzu
