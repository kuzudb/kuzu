#pragma once

#include "src/function/include/unary_operation_executor.h"

namespace kuzu {
namespace function {

struct NullOperationExecutor {

    template<typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        assert(result.dataType.typeID == BOOL);
        result.state = operand.state;
        auto operandValues = (uint8_t*)operand.values;
        auto resultValues = (uint8_t*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            FUNC::operation(operandValues[pos], (bool)operand.isNull(pos), resultValues[pos]);
        } else {
            if (operand.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    FUNC::operation(operandValues[i], (bool)operand.isNull(i), resultValues[i]);
                }
            } else {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    auto pos = operand.state->selVector->selectedPositions[i];
                    FUNC::operation(
                        operandValues[pos], (bool)operand.isNull(pos), resultValues[pos]);
                }
            }
        }
    }

    template<typename FUNC>
    static bool select(ValueVector& operand, SelectionVector& selVector) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue = 0;
            FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            return resultValue == true;
        } else {
            uint64_t numSelectedValues = 0;
            auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
            for (auto i = 0ul; i < operand.state->selVector->selectedSize; i++) {
                auto pos = operand.state->selVector->selectedPositions[i];
                selectOnValue<FUNC>(operand, pos, numSelectedValues, selectedPositionsBuffer);
            }
            selVector.selectedSize = numSelectedValues;
            return numSelectedValues > 0;
        }
    }

    template<typename FUNC>
    static void selectOnValue(ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        auto operandValues = operand.values;
        FUNC::operation(operandValues[operandPos], operand.isNull(operandPos), resultValue);
        selectedPositionsBuffer[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }
};

} // namespace function
} // namespace kuzu
