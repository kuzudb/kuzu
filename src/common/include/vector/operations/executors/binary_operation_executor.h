#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    // A (left operand type), B (right operand type), R (result type)
    template<class A, class B, class R, class FUNC = function<R(A, B)>>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.getValues();
        auto rValues = (B*)right.getValues();
        auto resultValues = (R*)result.getValues();
        if (left.isFlat() && right.isFlat()) {
            auto lValue = lValues[left.getCurrSelectedValuesPos()];
            auto rValue = rValues[right.getCurrSelectedValuesPos()];
            resultValues[0] = FUNC::operation(lValue, rValue);
        } else if (left.isFlat()) {
            auto size = right.getNumSelectedValues();
            auto lValue = lValues[left.getCurrSelectedValuesPos()];
            auto selectedValuesPos = right.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(lValue, rValues[selectedValuesPos[i]]);
            }
        } else if (right.isFlat()) {
            auto size = left.getNumSelectedValues();
            auto rValue = rValues[right.getCurrSelectedValuesPos()];
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                resultValues[i] = FUNC::operation(lValues[selectedValuesPos[i]], rValue);
            }
        } else {
            auto size = left.getNumSelectedValues();
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[i] = FUNC::operation(lValues[pos], rValues[pos]);
            }
        }
    }

    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static void executeOnNodeIDs(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto resultValues = result.getValues();
        nodeID_t lNodeID, rNodeID;
        if (left.isFlat() && right.isFlat()) {
            left.readNodeOffsetAndLabel(left.getCurrSelectedValuesPos(), lNodeID);
            right.readNodeOffsetAndLabel(right.getCurrSelectedValuesPos(), rNodeID);
            resultValues[0] = FUNC::operation(lNodeID, rNodeID);
        } else if (left.isFlat()) {
            auto size = right.size();
            auto selectedValuesPos = right.getSelectedValuesPos();
            left.readNodeOffsetAndLabel(left.getCurrSelectedValuesPos(), lNodeID);
            for (uint64_t i = 0; i < size; i++) {
                right.readNodeOffsetAndLabel(selectedValuesPos[i], rNodeID);
                resultValues[i] = FUNC::operation(lNodeID, rNodeID);
            }
        } else if (right.isFlat()) {
            auto size = left.size();
            auto selectedValuesPos = left.getSelectedValuesPos();
            right.readNodeOffsetAndLabel(right.getCurrSelectedValuesPos(), rNodeID);
            for (uint64_t i = 0; i < size; i++) {
                left.readNodeOffsetAndLabel(selectedValuesPos[i], lNodeID);
                resultValues[i] = FUNC::operation(lNodeID, rNodeID);
            }
        } else {
            auto size = left.size();
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                left.readNodeOffsetAndLabel(pos, lNodeID);
                right.readNodeOffsetAndLabel(pos, rNodeID);
                resultValues[i] = FUNC::operation(lNodeID, rNodeID);
            }
        }
    }
};

} // namespace common
} // namespace graphflow
