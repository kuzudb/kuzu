#pragma once

#include <functional>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

struct UnaryOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(
        OPERAND_TYPE& input, RESULT_TYPE& result, void* inputVector, void* resultVector) {
        FUNC::operation(input, result);
    }
};

struct UnaryStringOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(
        OPERAND_TYPE& input, RESULT_TYPE& result, void* inputVector, void* resultVector) {
        FUNC::operation(input, result, *(common::ValueVector*)resultVector);
    }
};

struct UnaryListOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(
        OPERAND_TYPE& input, RESULT_TYPE& result, void* leftValueVector, void* resultValueVector) {
        FUNC::operation(input, result, *(common::ValueVector*)leftValueVector,
            *(common::ValueVector*)resultValueVector);
    }
};

struct UnaryCastOperationWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(
        OPERAND_TYPE& input, RESULT_TYPE& result, void* inputVector, void* resultVector) {
        FUNC::operation(
            input, result, *(common::ValueVector*)inputVector, *(common::ValueVector*)resultVector);
    }
};

struct UnaryOperationExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& operand, uint64_t operandPos,
        RESULT_TYPE& resultValue, common::ValueVector& resultValueVector) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
            ((OPERAND_TYPE*)operand.getData())[operandPos], resultValue, (void*)&operand,
            (void*)&resultValueVector);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& operand, common::ValueVector& result) {
        result.resetAuxiliaryBuffer();
        auto resultValues = (RESULT_TYPE*)result.getData();
        if (operand.state->isFlat()) {
            auto inputPos = operand.state->selVector->selectedPositions[0];
            auto resultPos = result.state->selVector->selectedPositions[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(inputPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                    operand, inputPos, resultValues[resultPos], result);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, i, resultValues[i], result);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, pos, resultValues[pos], result);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, i, resultValues[i], result);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
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
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryOperationWrapper>(operand, result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeString(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryStringOperationWrapper>(
            operand, result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeListStruct(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryListOperationWrapper>(operand, result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeCast(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastOperationWrapper>(operand, result);
    }
};

} // namespace function
} // namespace kuzu
