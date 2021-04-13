#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    // A (left operand type), B (right operand type), R (result type)
    template<class A, class B, class R, class FUNC = function<R(A, B)>>
    static void executeNonBoolOp(ValueVector& left, ValueVector& right, ValueVector& result) {
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
                    resultValues[pos] = FUNC::operation(lValues[pos], rValue);
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
    static void executeBoolOp(ValueVector& left, ValueVector& right, ValueVector& result) {
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.values[resPos] = FUNC::operation(
                left.values[lPos], right.values[rPos], left.nullMask[lPos], right.nullMask[rPos]);
            result.nullMask[resPos] = result.values[resPos] == NULL_BOOL;
        } else if (left.state->isFlat()) {
            auto size = right.state->numSelectedValues;
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto lValue = left.values[lPos];
            auto isLeftNull = left.nullMask[lPos];
            // right and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = right.state->selectedValuesPos[i];
                result.values[pos] =
                    FUNC::operation(lValue, right.values[pos], isLeftNull, right.nullMask[pos]);
                result.nullMask[pos] = result.values[pos] == NULL_BOOL;
            }
        } else if (right.state->isFlat()) {
            auto size = left.state->numSelectedValues;
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto rValue = right.values[rPos];
            auto isRightNull = right.nullMask[rPos];
            // left and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.values[pos] =
                    FUNC::operation(left.values[pos], rValue, left.nullMask[pos], isRightNull);
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
        nodeID_t lNodeID{}, rNodeID{};
        if (left.state->isFlat() && right.state->isFlat()) {
            lNodeIDVector.readNodeOffsetAndLabel(left.state->getCurrSelectedValuesPos(), lNodeID);
            rNodeIDVector.readNodeOffsetAndLabel(right.state->getCurrSelectedValuesPos(), rNodeID);
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[left.state->getCurrSelectedValuesPos()] ||
                                      right.nullMask[right.state->getCurrSelectedValuesPos()];
            if (!result.nullMask[resPos]) { // not NULL.
                result.values[resPos] = FUNC::operation(lNodeID, rNodeID);
            }
        } else if (left.state->isFlat()) {
            auto size = right.state->numSelectedValues;
            auto lNullMask = left.nullMask[left.state->getCurrSelectedValuesPos()];
            // right and result vectors share the same selectedValuesPos.
            lNodeIDVector.readNodeOffsetAndLabel(left.state->getCurrSelectedValuesPos(), lNodeID);
            for (uint64_t i = 0; i < size; i++) {
                auto pos = right.state->selectedValuesPos[i];
                rNodeIDVector.readNodeOffsetAndLabel(pos, rNodeID);
                result.nullMask[i] =
                    lNullMask || right.nullMask[right.state->getCurrSelectedValuesPos()];
                if (!result.nullMask[i]) { // not NULL.
                    result.values[i] = FUNC::operation(lNodeID, rNodeID);
                }
            }
        } else if (right.state->isFlat()) {
            auto size = left.state->numSelectedValues;
            auto rNullMask = right.nullMask[left.state->getCurrSelectedValuesPos()];
            // left and result vectors share the same selectedValuesPos.
            rNodeIDVector.readNodeOffsetAndLabel(right.state->getCurrSelectedValuesPos(), rNodeID);
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                lNodeIDVector.readNodeOffsetAndLabel(pos, lNodeID);
                result.nullMask[pos] =
                    left.nullMask[left.state->getCurrSelectedValuesPos()] || rNullMask;
                if (!result.nullMask[pos]) { // not NULL.
                    result.values[pos] = FUNC::operation(lNodeID, rNodeID);
                }
            }
        } else {
            auto size = left.state->numSelectedValues;
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                lNodeIDVector.readNodeOffsetAndLabel(pos, lNodeID);
                lNodeIDVector.readNodeOffsetAndLabel(pos, rNodeID);
                result.nullMask[pos] = left.nullMask[left.state->getCurrSelectedValuesPos()] ||
                                       right.nullMask[right.state->getCurrSelectedValuesPos()];
                if (!result.nullMask[pos]) { // not NULL.
                    result.values[pos] = FUNC::operation(lNodeID, rNodeID);
                }
            }
        }
    }
};

} // namespace common
} // namespace graphflow
