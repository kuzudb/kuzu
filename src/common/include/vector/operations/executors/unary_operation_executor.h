#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<class T, class R, class FUNC = std::function<R(T)>>
    static void executeNonBoolOp(ValueVector& operand, ValueVector& result) {
        auto values = (T*)operand.values;
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            result.nullMask[pos] = operand.nullMask[pos];
            if (!result.nullMask[pos]) { // not NULL.
                resultValues[pos] = FUNC::operation(values[pos]);
            }
        } else {
            auto size = operand.state->numSelectedValues;
            for (uint64_t i = 0; i < size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.nullMask[pos] = operand.nullMask[pos];
                if (!result.nullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(values[pos]);
                }
            }
        }
    }

    template<class FUNC = std::function<uint8_t(uint8_t)>>
    static void executeBoolOp(ValueVector& operand, ValueVector& result) {
        auto values = operand.values;
        auto resultValues = result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            resultValues[pos] = FUNC::operation(values[pos], operand.nullMask[pos]);
            result.nullMask[pos] = resultValues[pos] == NULL_BOOL;
        } else {
            auto size = operand.state->numSelectedValues;
            for (uint64_t i = 0; i < size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                resultValues[pos] = FUNC::operation(values[pos], operand.nullMask[pos]);
                result.nullMask[pos] = resultValues[pos] == NULL_BOOL;
            }
        }
    }

    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto& nodeIDVector = (NodeIDVector&)operand;
        auto resultValues = (R*)result.values;
        nodeID_t nodeID{};
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            nodeIDVector.readNodeOffsetAndLabel(pos, nodeID);
            resultValues[pos] = FUNC::operation(nodeID);
            result.nullMask[pos] = operand.nullMask[pos];
        } else {
            auto size = operand.state->numSelectedValues;
            for (uint64_t i = 0; i < size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                nodeIDVector.readNodeOffsetAndLabel(pos, nodeID);
                resultValues[pos] = FUNC::operation(nodeID);
                result.nullMask[pos] = operand.nullMask[pos];
            }
        }
    }

    template<bool value>
    static void nullMaskCmp(ValueVector& operand, ValueVector& result) {
        auto resultValues = result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            resultValues[pos] = operand.nullMask[pos] == value;
        } else {
            auto size = operand.state->numSelectedValues;
            for (uint64_t i = 0; i < size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                resultValues[pos] = operand.nullMask[pos] == value;
            }
        }
    }
};

} // namespace common
} // namespace graphflow
