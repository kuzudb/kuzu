#pragma once

#include <functional>

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<typename T, typename R, class FUNC = std::function<R(T)>, bool IS_NODE_ID>
    static void executeOnTuple(ValueVector& operand, T* operandValues, R* resultValues,
        uint64_t operandPos, uint64_t resultPos) {
        if constexpr (IS_NODE_ID) {
            nodeID_t nodeID;
            operand.readNodeID(operandPos, nodeID);
            FUNC::operation(nodeID, (bool)operand.isNull(operandPos), resultValues[resultPos]);
        } else {
            FUNC::operation(operandValues[operandPos], (bool)operand.isNull(operandPos),
                resultValues[resultPos]);
        }
    }

    template<class T, class R, class FUNC = std::function<R(T)>, bool IS_NODE_ID, bool SKIP_NULL>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto operandValues = (T*)operand.values;
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            if constexpr (SKIP_NULL) {
                if (!operand.isNull(operandPos)) {
                    executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                        operand, operandValues, resultValues, operandPos, resultPos);
                }
            } else {
                executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                    operand, operandValues, resultValues, operandPos, resultPos);
            }
        } else {
            if constexpr (SKIP_NULL) {
                if (operand.hasNoNullsGuarantee()) {
                    if (operand.state->isUnfiltered()) {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                                operand, operandValues, resultValues, i, i);
                        }
                    } else {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            auto pos = operand.state->selectedPositions[i];
                            executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                                operand, operandValues, resultValues, pos, pos);
                        }
                    }
                } else {
                    if (operand.state->isUnfiltered()) {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            if (!operand.isNull(i)) {
                                executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                                    operand, operandValues, resultValues, i, i);
                            }
                        }
                    } else {
                        for (auto i = 0u; i < operand.state->selectedSize; i++) {
                            auto pos = operand.state->selectedPositions[i];
                            if (!operand.isNull(pos)) {
                                executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                                    operand, operandValues, resultValues, pos, pos);
                            }
                        }
                    }
                }
            } else {
                if (operand.state->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                            operand, operandValues, resultValues, i, i);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selectedSize; i++) {
                        auto pos = operand.state->selectedPositions[i];
                        executeOnTuple<T, R, FUNC, IS_NODE_ID>(
                            operand, operandValues, resultValues, pos, pos);
                    }
                }
            }
        }
    }

    // NOT
    template<class FUNC = function<uint8_t(uint8_t, bool)>>
    static uint64_t selectBooleanOps(ValueVector& operand, sel_t* selectedPositions) {
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

    // IS NULL, IS NOT NULL
    template<bool value>
    static uint64_t selectOnNullMask(ValueVector& operand, sel_t* selectedPositions) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            auto result = operand.isNull(pos) == value;
            return result == TRUE;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                auto result = operand.isNull(pos) == value;
                selectedPositions[numSelectedValues] = pos;
                numSelectedValues += result;
            }
            return numSelectedValues;
        }
    }
};

} // namespace common
} // namespace graphflow
