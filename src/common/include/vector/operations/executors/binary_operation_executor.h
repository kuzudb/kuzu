#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    // A (left operand type), B (right operand type), R (result type)
    template<class A, class B, class R, class FUNC = function<R(A, B)>>
    static void executeArithmeticAndComparisonOperations(
        ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        auto resultValues = (R*)result.values;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[lPos] || right.nullMask[rPos];
            if (!result.nullMask[resPos]) { // not NULL.
                resultValues[resPos] = FUNC::operation(lValues[lPos], rValues[rPos]);
            }
        } else if (left.state->isFlat()) {
            auto size = right.state->numSelectedValues;
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto lValue = lValues[lPos];
            auto isLeftNull = left.nullMask[lPos];
            // right and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = right.state->selectedValuesPos[i];
                result.nullMask[pos] = isLeftNull || right.nullMask[pos];
                if (!result.nullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lValue, rValues[pos]);
                }
            }
        } else if (right.state->isFlat()) {
            auto size = left.state->numSelectedValues;
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto rValue = rValues[rPos];
            auto isRightNull = right.nullMask[rPos];
            // left and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || isRightNull;
                if (!result.nullMask[i]) { // not NULL.
                    auto lVal = lValues[pos];
                    auto res = FUNC::operation(lVal, rValue);
                    resultValues[pos] = res; // FUNC::operation(lValues[pos], rValue);
                }
            }
        } else {
            auto size = left.state->numSelectedValues;
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || right.nullMask[pos];
                if (!result.nullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lValues[pos], rValues[pos]);
                }
            }
        }
    }

    // A (left operand type), B (right operand type), R (result type)
    template<class FUNC = function<uint8_t(uint8_t, uint8_t, bool, bool)>>
    static void executeBoolOperation(ValueVector& left, ValueVector& right, ValueVector& result) {
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.values[resPos] = FUNC::operation(
                left.values[lPos], right.values[rPos], left.nullMask[lPos], right.nullMask[rPos]);
            result.nullMask[resPos] = result.values[resPos] == NULL_BOOL;
        } else if (left.state->isFlat() || right.state->isFlat()) {
            auto& flatVector = left.state->isFlat() ? left : right;
            auto& unflatVector = !left.state->isFlat() ? left : right;
            auto pos = flatVector.state->getCurrSelectedValuesPos();
            auto flatValue = flatVector.values[pos];
            auto isFlatNull = left.nullMask[pos];
            // unflat and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < unflatVector.state->numSelectedValues; i++) {
                pos = unflatVector.state->selectedValuesPos[i];
                result.values[pos] = FUNC::operation(
                    flatValue, unflatVector.values[pos], isFlatNull, unflatVector.nullMask[pos]);
                result.nullMask[pos] = result.values[pos] == NULL_BOOL;
            }
        } else {
            auto size = left.state->numSelectedValues;
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.values[pos] = FUNC::operation(
                    left.values[pos], right.values[pos], left.nullMask[pos], right.nullMask[pos]);
                result.nullMask[pos] = result.values[pos] == NULL_BOOL;
            }
        }
    }

    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static void executeOnNodeIDs(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto& lNodeIDVector = (NodeIDVector&)left;
        auto& rNodeIDVector = (NodeIDVector&)right;
        nodeID_t nodeID{}, otherNodeID{};
        if (left.state->isFlat() && right.state->isFlat()) {
            lNodeIDVector.readNodeOffsetAndLabel(left.state->getCurrSelectedValuesPos(), nodeID);
            rNodeIDVector.readNodeOffsetAndLabel(
                right.state->getCurrSelectedValuesPos(), otherNodeID);
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[left.state->getCurrSelectedValuesPos()] ||
                                      right.nullMask[right.state->getCurrSelectedValuesPos()];
            if (!result.nullMask[resPos]) { // not NULL.
                result.values[resPos] = FUNC::operation(nodeID, otherNodeID);
            }
        } else if (left.state->isFlat() || right.state->isFlat()) {
            auto& flatVector = left.state->isFlat() ? lNodeIDVector : rNodeIDVector;
            auto& unflatVector = !left.state->isFlat() ? lNodeIDVector : rNodeIDVector;
            auto size = unflatVector.state->numSelectedValues;
            auto isFlatNull = flatVector.nullMask[flatVector.state->getCurrSelectedValuesPos()];
            // unflat and result vectors share the same selectedValuesPos.
            flatVector.readNodeOffsetAndLabel(flatVector.state->getCurrSelectedValuesPos(), nodeID);
            for (uint64_t i = 0; i < size; i++) {
                auto pos = unflatVector.state->selectedValuesPos[i];
                unflatVector.readNodeOffsetAndLabel(pos, otherNodeID);
                result.nullMask[i] =
                    isFlatNull ||
                    unflatVector.nullMask[unflatVector.state->getCurrSelectedValuesPos()];
                if (!result.nullMask[i]) { // not NULL.
                    result.values[i] = FUNC::operation(nodeID, otherNodeID);
                }
            }
        } else {
            auto size = left.state->numSelectedValues;
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                lNodeIDVector.readNodeOffsetAndLabel(pos, nodeID);
                lNodeIDVector.readNodeOffsetAndLabel(pos, otherNodeID);
                result.nullMask[pos] = left.nullMask[left.state->getCurrSelectedValuesPos()] ||
                                       right.nullMask[right.state->getCurrSelectedValuesPos()];
                if (!result.nullMask[pos]) { // not NULL.
                    result.values[pos] = FUNC::operation(nodeID, otherNodeID);
                }
            }
        }
    }
};

} // namespace common
} // namespace graphflow
