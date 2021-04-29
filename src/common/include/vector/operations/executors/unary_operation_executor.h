#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<class T, class R, class FUNC = std::function<R(T)>>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto inputValues = (T*)operand.values;
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            if (!operand.nullMask[pos]) {
                resultValues[pos] = FUNC::operation(inputValues[pos]);
            }
        } else {
            for (auto i = 0ul; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                if (!operand.nullMask[pos]) {
                    resultValues[pos] = FUNC::operation(inputValues[pos]);
                }
            }
        }
    }

    template<class FUNC = std::function<uint8_t(uint8_t, uint8_t)>>
    static void executeBoolOperation(ValueVector& operand, ValueVector& result) {
        auto inputValues = operand.values;
        auto resultValues = result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            resultValues[pos] = FUNC::operation(inputValues[pos], operand.nullMask[pos]);
        } else {
            for (auto i = 0ul; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                resultValues[pos] = FUNC::operation(inputValues[pos], operand.nullMask[pos]);
            }
        }
    }

    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto& nodeIDVector = (NodeIDVector&)operand;
        auto resultValues = (R*)result.values;
        nodeID_t nodeID;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getCurrSelectedValuesPos();
            nodeIDVector.readNodeOffsetAndLabel(pos, nodeID);
            if (!operand.nullMask[pos]) {
                resultValues[pos] = FUNC::operation(nodeID);
            }
        } else {
            for (auto i = 0ul; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                nodeIDVector.readNodeOffsetAndLabel(pos, nodeID);
                if (!operand.nullMask[pos]) {
                    resultValues[pos] = FUNC::operation(nodeID);
                }
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
            for (auto i = 0ul; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                resultValues[pos] = operand.nullMask[pos] == value;
            }
        }
    }
};

} // namespace common
} // namespace graphflow
