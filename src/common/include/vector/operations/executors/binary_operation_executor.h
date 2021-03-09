#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    // A (left operand type), B (right operand type), R (result type)
    template<class A, class B, class R, class FUNC = function<R(A, B)>>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.getValues();
        auto rValues = (B*)right.getValues();
        auto resultValues = (R*)result.getValues();
        if (left.isFlat() && right.isFlat()) {
            resultValues[0] =
                FUNC::operation(lValues[left.getCurrPos()], rValues[right.getCurrPos()]);
        } else if (left.isFlat()) {
            auto size = right.size();
            auto lValue = lValues[left.getCurrPos()];
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(lValue, rValues[i]);
            }
        } else if (right.isFlat()) {
            auto size = left.size();
            auto rValue = rValues[right.getCurrPos()];
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(lValues[i], rValue);
            }
        } else {
            auto size = left.size();
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(lValues[i], rValues[i]);
            }
        }
    }
};

} // namespace common
} // namespace graphflow
