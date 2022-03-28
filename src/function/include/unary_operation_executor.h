#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"
#include "src/function/boolean/operations/include/boolean_operations.h"

namespace graphflow {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */
struct UnaryOperationExecutor {

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeOnValue(
        ValueVector& operand, uint64_t operandPos, RESULT_TYPE& resultValue) {
        auto operandValues = (OPERAND_TYPE*)operand.values.get();
        FUNC::operation(operandValues[operandPos], (bool)operand.isNull(operandPos), resultValue);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        result.resetOverflowBuffer();
        auto resultValues = (RESULT_TYPE*)result.values.get();
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            assert(pos == result.state->getPositionOfCurrIdx());
            result.setNull(pos, operand.isNull(pos));
            if (!result.isNull(pos)) {
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
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, i, resultValues[i]);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, pos, resultValues[pos]);
                        }
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename FUNC>
    static void selectOnValue(ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, sel_t* selectedPositions) {
        uint8_t resultValue = 0;
        auto operandValues = (OPERAND_TYPE*)operand.values.get();
        FUNC::operation(operandValues[operandPos], operand.isNull(operandPos), resultValue);
        selectedPositions[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }

    // NOT
    template<typename OPERAND_TYPE, typename FUNC>
    static uint64_t select(ValueVector& operand, sel_t* selectedPositions) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue = 0;
            if (!operand.isNull(pos)) {
                FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            }
            return resultValue == true;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                if (!operand.isNull(pos)) {
                    selectOnValue<OPERAND_TYPE, FUNC>(
                        operand, pos, numSelectedValues, selectedPositions);
                }
            }
            return numSelectedValues;
        }
    }
};

} // namespace function
} // namespace graphflow
