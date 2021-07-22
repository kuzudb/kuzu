#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<class T, class R, class FUNC = std::function<R(T)>>
    static void executeArithmeticOps(ValueVector& operand, ValueVector& result) {
        auto inputValues = (T*)operand.values;
        auto resultValues = (R*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            if (!operand.nullMask[pos]) {
                FUNC::operation(inputValues[pos], resultValues[0]);
            }
        } else {
            for (auto i = 0ul; i < operand.state->size; i++) {
                auto pos = operand.state->getSelectedPositionAtIdx(i);
                if (!operand.nullMask[pos]) {
                    FUNC::operation(inputValues[pos], resultValues[pos]);
                }
            }
        }
    }

    template<class T, class FUNC = std::function<uint64_t(T)>>
    static void executeHashOps(ValueVector& operand, ValueVector& result) {
        auto inputValues = (T*)operand.values;
        auto resultValues = (uint64_t*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            if (!operand.nullMask[pos]) {
                resultValues[pos] = FUNC::operation(inputValues[pos]);
            }
        } else {
            for (auto i = 0ul; i < operand.state->size; i++) {
                auto pos = operand.state->getSelectedPositionAtIdx(i);
                if (!operand.nullMask[pos]) {
                    resultValues[pos] = FUNC::operation(inputValues[pos]);
                }
            }
        }
    }

    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto resultValues = (R*)result.values;
        nodeID_t nodeID;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            operand.readNodeID(pos, nodeID);
            if (!operand.nullMask[pos]) {
                resultValues[pos] = FUNC::operation(nodeID);
            }
        } else {
            for (auto i = 0ul; i < operand.state->size; i++) {
                auto pos = operand.state->getSelectedPositionAtIdx(i);
                operand.readNodeID(pos, nodeID);
                if (!operand.nullMask[pos]) {
                    resultValues[pos] = FUNC::operation(nodeID);
                }
            }
        }
    }

    template<bool value>
    static void nullMaskCmp(ValueVector& operand, ValueVector& result) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            result.values[pos] = operand.nullMask[pos] == value;
        } else {
            for (auto i = 0ul; i < operand.state->size; i++) {
                auto pos = operand.state->getSelectedPositionAtIdx(i);
                result.values[pos] = operand.nullMask[pos] == value;
            }
        }
    }
};

} // namespace common
} // namespace graphflow
