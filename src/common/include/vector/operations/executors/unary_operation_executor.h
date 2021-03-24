#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    // O (operand type), R (result type)
    template<class A, class R, class FUNC = std::function<R(A)>>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto values = (A*)operand.getValues();
        auto resultValues = (R*)result.getValues();
        if (operand.isFlat()) {
            resultValues[0] = FUNC::operation(values[operand.owner->getCurrSelectedValuesPos()]);
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            auto size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(values[selectedValuesPos[i]]);
            }
        }
    }

    // A (operand type), R (result type)
    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto resultValues = (R*)result.getValues();
        nodeID_t nodeID;
        if (operand.isFlat()) {
            nodeID_t nodeID;
            operand.readNodeOffsetAndLabel(operand.owner->getCurrSelectedValuesPos(), nodeID);
            resultValues[0] = FUNC::operation(nodeID);
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            auto size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                operand.readNodeOffsetAndLabel(selectedValuesPos[i], nodeID);
                resultValues[i] = FUNC::operation(nodeID);
            }
        }
    }
};

} // namespace common
} // namespace graphflow
