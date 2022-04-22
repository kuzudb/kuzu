#pragma once

#include "src/function/include/unary_operation_executor.h"

namespace graphflow {
namespace function {

struct NullOperationExecutor {

    template<typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        assert(result.dataType.typeID == BOOL);
        auto resultValues = (uint8_t*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            assert(pos == result.state->getPositionOfCurrIdx());
            UnaryOperationExecutor::executeOnValue<uint8_t, uint8_t, FUNC, UnaryOperationWrapper>(
                operand, pos, resultValues[pos], result);
        } else {
            if (operand.state->isUnfiltered()) {
                for (auto i = 0u; i < operand.state->selectedSize; i++) {
                    UnaryOperationExecutor::executeOnValue<
                        uint8_t /* operand type does not matter for null operations */, uint8_t,
                        FUNC, UnaryOperationWrapper>(operand, i, resultValues[i], result);
                }
            } else {
                for (auto i = 0u; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    UnaryOperationExecutor::executeOnValue<
                        uint8_t /* operand type does not matter for null operations */, uint8_t,
                        FUNC, UnaryOperationWrapper>(operand, pos, resultValues[pos], result);
                }
            }
        }
    }

    template<typename FUNC>
    static uint64_t select(ValueVector& operand, sel_t* selectedPositions) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue = 0;
            FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            return resultValue == true;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                UnaryOperationExecutor::selectOnValue<
                    uint8_t /* operand type does not matter for null operations */, FUNC>(
                    operand, pos, numSelectedValues, selectedPositions);
            }
            return numSelectedValues;
        }
    }
};

} // namespace function
} // namespace graphflow
