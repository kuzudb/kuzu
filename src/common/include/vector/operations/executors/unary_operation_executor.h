#pragma once

#include <functional>

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<typename T, typename R, typename FUNC = std::function<R(T)>>
    static void executeOnTuple(ValueVector& operand, R& resultValue, uint64_t operandPos) {
        if constexpr ((is_same<T, nodeID_t>::value)) {
            nodeID_t nodeID;
            operand.readNodeID(operandPos, nodeID);
            FUNC::operation(nodeID, (bool)operand.isNull(operandPos), resultValue);
        } else {
            auto operandValues = (T*)operand.values;
            FUNC::operation(
                operandValues[operandPos], (bool)operand.isNull(operandPos), resultValue);
        }
    }

    template<typename T, typename R, typename FUNC = std::function<R(T)>, bool SKIP_NULL = true>
    static void execute(ValueVector& operand, ValueVector& result) {
        // SKIP_NULL is set to false ONLY when the FUNC is boolean operations.
        assert(SKIP_NULL ||
               (!SKIP_NULL && (is_same<T, uint8_t>::value) && (is_same<R, uint8_t>::value)));
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            if constexpr (SKIP_NULL) {
                if (!operand.isNull(operandPos)) {
                    executeOnTuple<T, R, FUNC>(operand, resultValues[resultPos], operandPos);
                }
            } else {
                executeOnTuple<T, R, FUNC>(operand, resultValues[resultPos], operandPos);
            }
        } else {
            if constexpr (SKIP_NULL) {
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
    template<typename FUNC = function<uint8_t(uint8_t, bool)>>
    static uint64_t select(ValueVector& operand, sel_t* selectedPositions) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue;
            FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            return resultValue == TRUE;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                uint8_t resultValue;
                FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
                selectedPositions[numSelectedValues] = pos;
                numSelectedValues += resultValue;
            }
            return numSelectedValues;
        }
    }
};

} // namespace common
} // namespace graphflow
