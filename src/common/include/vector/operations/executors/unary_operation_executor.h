#pragma once

#include <functional>

#include "src/common/include/operations/boolean_operations.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<typename T, typename R, typename FUNC>
    static void executeOnTuple(ValueVector& operand, R& resultValue, uint64_t operandPos) {
        if constexpr ((is_same<T, nodeID_t>::value)) {
            nodeID_t nodeID{};
            operand.readNodeID(operandPos, nodeID);
            FUNC::operation(nodeID, (bool)operand.isNull(operandPos), resultValue);
        } else {
            auto operandValues = (T*)operand.values;
            FUNC::operation(
                operandValues[operandPos], (bool)operand.isNull(operandPos), resultValue);
        }
    }

    template<typename T, typename R, typename FUNC, bool IS_BOOL_OP = false>
    static void execute(ValueVector& operand, ValueVector& result) {
        assert(!IS_BOOL_OP ||
               (IS_BOOL_OP && (is_same<T, bool>::value) && (is_same<R, uint8_t>::value)));
        result.resetStringBuffer();
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            if constexpr (!IS_BOOL_OP) {
                if (!operand.isNull(operandPos)) {
                    executeOnTuple<T, R, FUNC>(operand, resultValues[resultPos], operandPos);
                }
            } else {
                executeOnTuple<T, R, FUNC>(operand, resultValues[resultPos], operandPos);
            }
        } else {
            if constexpr (!IS_BOOL_OP) {
                if (operand.hasNoNullsGuarantee()) {
                    if (operand.state->isUnfiltered()) {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            executeOnTuple<T, R, FUNC>(operand, resultValues[i], i);
                        }
                    } else {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            auto pos = operand.state->selectedPositions[i];
                            executeOnTuple<T, R, FUNC>(operand, resultValues[pos], pos);
                        }
                    }
                } else {
                    if (operand.state->isUnfiltered()) {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            if (!operand.isNull(i)) {
                                executeOnTuple<T, R, FUNC>(operand, resultValues[i], i);
                            }
                        }
                    } else {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            auto pos = operand.state->selectedPositions[i];
                            if (!operand.isNull(pos)) {
                                executeOnTuple<T, R, FUNC>(operand, resultValues[pos], pos);
                            }
                        }
                    }
                }
            } else {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        executeOnTuple<T, R, FUNC>(operand, resultValues[i], i);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        executeOnTuple<T, R, FUNC>(operand, resultValues[pos], pos);
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
            uint8_t resultValue;
            FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            return resultValue != operation::NULL_BOOL && resultValue != false ? 1 : 0;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                uint8_t resultValue;
                FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
                selectedPositions[numSelectedValues] = pos;
                numSelectedValues +=
                    resultValue != operation::NULL_BOOL && resultValue != false ? 1 : 0;
            }
            return numSelectedValues;
        }
    }
};

} // namespace common
} // namespace graphflow
