#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    // ARITHMETIC (NEGATE, ABS), HASH
    template<class T, class R, class FUNC = std::function<R(T)>>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto inputValues = (T*)operand.values;
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            if (!operand.isNull(operandPos)) {
                FUNC::operation(inputValues[operandPos], resultValues[resultPos]);
            }
        } else {
            // The operand and result share the same nullMasks
            if (operand.hasNoNullsGuarantee()) {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    FUNC::operation(inputValues[pos], resultValues[pos]);
                }
            } else {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    if (!operand.isNull(pos)) {
                        FUNC::operation(inputValues[pos], resultValues[pos]);
                    }
                }
            }
        }
    }

    // NOT
    template<class FUNC = function<uint8_t(uint8_t, bool)>>
    static void executeBooleanOps(ValueVector& operand, ValueVector& result) {
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            result.values[resultPos] =
                FUNC::operation(operand.values[operandPos], operand.isNull(operandPos));
        } else {
            // result and operand share the same selectedPositions.
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.values[pos] = FUNC::operation(operand.values[pos], operand.isNull(pos));
            }
        }
    }

    // IS NULL, IS NOT NULL
    template<bool value>
    static void executeOnNullMask(ValueVector& operand, ValueVector& result) {
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            result.values[resultPos] = operand.isNull(operandPos) == value;
        } else {
            // result and operand share the same selectedPositions.
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.values[pos] = operand.isNull(pos) == value;
            }
        }
    }

    // Specialized function for nodeID_t.
    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeNodeIDOps(ValueVector& operand, ValueVector& result) {
        auto resultValues = (R*)result.values;
        nodeID_t nodeID;
        if (operand.state->isFlat()) {
            auto operandPos = operand.state->getPositionOfCurrIdx();
            auto resultPos = result.state->getPositionOfCurrIdx();
            operand.readNodeID(operandPos, nodeID);
            if (!operand.isNull(operandPos)) {
                FUNC::operation(nodeID, resultValues[resultPos]);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    operand.readNodeID(pos, nodeID);
                    FUNC::operation(nodeID, resultValues[pos]);
                }
            } else {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    operand.readNodeID(pos, nodeID);
                    if (!operand.isNull(pos)) {
                        FUNC::operation(nodeID, resultValues[pos]);
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
            uint8_t resultValue = FUNC::operation(operand.values[pos], operand.isNull(pos));
            return resultValue == TRUE;
        } else {
            uint64_t numSelectedValues = 0;
            for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                uint8_t resultValue = FUNC::operation(operand.values[pos], operand.isNull(pos));
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
