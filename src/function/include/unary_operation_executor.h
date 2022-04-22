#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

// Forward declaration of string operations that returns a string.
namespace operation {
class Lower;
class Upper;
class Ltrim;
class Rtrim;
class Reverse;
} // namespace operation

struct UnaryOperationExecutor {

    template<class FUNC>
    static inline constexpr bool isFuncResultStr() {
        return is_same<FUNC, operation::Lower>::value || is_same<FUNC, operation::Upper>::value ||
               is_same<FUNC, operation::Ltrim>::value || is_same<FUNC, operation::Rtrim>::value ||
               is_same<FUNC, operation::Reverse>::value;
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeOnValue(ValueVector& operand, uint64_t operandPos, RESULT_TYPE& resultValue,
        ValueVector& resultValueVector) {
        auto operandValues = (OPERAND_TYPE*)operand.values;
        if constexpr (isFuncResultStr<FUNC>()) {
            FUNC::operation(operandValues[operandPos], (bool)operand.isNull(operandPos),
                resultValue, resultValueVector);
        } else {
            FUNC::operation(
                operandValues[operandPos], (bool)operand.isNull(operandPos), resultValue);
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        result.resetOverflowBuffer();
        auto resultValues = (RESULT_TYPE*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            assert(pos == result.state->getPositionOfCurrIdx());
            result.setNull(pos, operand.isNull(pos));
            if (!result.isNull(pos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                    operand, pos, resultValues[pos], result);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                            operand, i, resultValues[i], result);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                            operand, pos, resultValues[pos], result);
                    }
                }
            } else {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, i, resultValues[i], result);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                                operand, pos, resultValues[pos], result);
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
