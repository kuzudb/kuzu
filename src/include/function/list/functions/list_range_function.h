#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct Range {
public:
    // range function:
    // - include end
    // - when start = end: there is only one element in result varlist
    // - when end - start are of opposite sign of step, the result will be empty
    // - default step = 1
    template<typename T>
    static inline void operation(T& start, T& end, common::list_entry_t& result,
        common::ValueVector& leftVector, common::ValueVector& /*rightVector*/,
        common::ValueVector& resultVector) {
        T step = 1;
        operation(start, end, step, result, leftVector, resultVector);
    }

    template<typename T>
    static inline void operation(T& start, T& end, T& step, common::list_entry_t& result,
        common::ValueVector& /*inputVector*/, common::ValueVector& resultVector) {
        if (step == 0) {
            throw common::RuntimeException("Step of range cannot be 0.");
        }

        // start, start + step, start + 2step, ..., end
        T number = start;
        auto size = (end - start) * 1.0 / step;
        size < 0 ? size = 0 : size = (int64_t)(size + 1);

        result = common::ListVector::addList(&resultVector, size);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        for (auto i = 0u; i < size; i++) {
            resultDataVector->setValue(result.offset + i, number);
            number += step;
        }
    }
};

} // namespace function
} // namespace kuzu
