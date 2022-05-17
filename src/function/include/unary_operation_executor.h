#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

struct UnaryOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(OPERAND_TYPE& input, bool isNull, RESULT_TYPE& result,
        void* dataptr, const DataType& inputType) {
        OP::operation(input, isNull, result);
    }
};

struct UnaryStringOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename OP>
    static void operation(OPERAND_TYPE& input, bool isNull, RESULT_TYPE& result, void* dataptr,
        const DataType& inputType) {
        OP::operation(input, isNull, result, *(ValueVector*)dataptr);
    }
};

struct UnaryCastOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename OP>
    static void operation(OPERAND_TYPE& input, bool isNull, RESULT_TYPE& result, void* dataptr,
        const DataType& inputType) {
        OP::operation(input, isNull, result, *(ValueVector*)dataptr, inputType);
    }
};

struct UnaryOperationExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(ValueVector& operand, uint64_t operandPos, RESULT_TYPE& resultValue,
        ValueVector& resultValueVector) {
        auto operandValues = (OPERAND_TYPE*)operand.values;
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(operandValues[operandPos],
            (bool)operand.isNull(operandPos), resultValue, (void*)&resultValueVector,
            operand.dataType);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(ValueVector& operand, ValueVector& result) {
        result.resetOverflowBuffer();
        result.state = operand.state;
        auto resultValues = (RESULT_TYPE*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            result.setNull(pos, operand.isNull(pos));
            if (!result.isNull(pos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                    operand, pos, resultValues[pos], result);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, i, resultValues[i], result);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, pos, resultValues[pos], result);
                    }
                }
            } else {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, i, resultValues[i], result);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, pos, resultValues[pos], result);
                        }
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryOperationWrapper>(operand, result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeString(ValueVector& operand, ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryStringOperationWrapper>(
            operand, result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeCast(ValueVector& operand, ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastOperationWrapper>(operand, result);
    }

    template<typename OPERAND_TYPE, typename FUNC>
    static void selectOnValue(ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, sel_t* selectedPositions) {
        uint8_t resultValue = 0;
        auto operandValues = (OPERAND_TYPE*)operand.values;
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
