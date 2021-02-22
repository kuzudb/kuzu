#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    // O (operand type), R (result type)
    template<class A, class R, class FUNC = std::function<R(A)>>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto values = (A*)operand.getValues();
        auto resultValues = (R*)result.getValues();
        if (operand.isFlat()) {
            resultValues[0] = FUNC::operation(values[operand.getCurrPos()]);
        } else {
            auto size = operand.size();
            for (uint64_t i = 0; i < size; i++) {
                std::cout << "values[" << i << "] = " << values[i] << std::endl;
                resultValues[i] = FUNC::operation(values[i]);
                std::cout << "resultValues[" << i << "] = " << resultValues[i] << std::endl;
            }
        }
    }
};

} // namespace common
} // namespace graphflow
