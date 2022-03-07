#pragma once

#include <functional>

#include "src/common/include/operations/boolean_operations.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeOnValue(
        ValueVector& operand, uint64_t operandPos, RESULT_TYPE& resultValue) {
        auto operandValues = (OPERAND_TYPE*)operand.values;
        FUNC::operation(operandValues[operandPos], (bool)operand.isNull(operandPos), resultValue);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        result.resetStringBuffer();
        auto resultValues = (RESULT_TYPE*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            assert(pos == result.state->getPositionOfCurrIdx());
            if (!operand.isNull(pos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(operand, pos, resultValues[pos]);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                            operand, i, resultValues[i]);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                            operand, pos, resultValues[pos]);
                    }
                }
            } else {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        if (!operand.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, i, resultValues[i]);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        if (!operand.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, pos, resultValues[pos]);
                        }
                    }
                }
            }
        }
    }

    // IS_NULL, IS_NOT_NULL, NOT
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
                uint8_t resultValue = 0;
                FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
                selectedPositions[numSelectedValues] = pos;
                numSelectedValues += resultValue == true;
            }
            return numSelectedValues;
        }
    }
};

} // namespace common
} // namespace graphflow
