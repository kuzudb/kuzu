#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    // A (left operand type), B (right operand type), R (result type)
    template<class A, class B, class R, class FUNC = function<R(A, B)>>
    static void executeNonBoolOp(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.getValues();
        auto rValues = (B*)right.getValues();
        auto resultValues = (R*)result.getValues();
        auto leftNullMask = left.getNullMask();
        auto rightNullMask = right.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        if (left.isFlat() && right.isFlat()) {
            auto lPos = left.getCurrSelectedValuesPos();
            auto rPos = right.getCurrSelectedValuesPos();
            auto resPos = result.getCurrSelectedValuesPos();
            resultNullMask[resPos] = leftNullMask[lPos] || rightNullMask[rPos];
            if (!resultNullMask[resPos]) { // not NULL.
                resultValues[resPos] = FUNC::operation(lValues[lPos], rValues[rPos]);
            }
            size = 1;
        } else if (left.isFlat()) {
            size = right.getNumSelectedValues();
            auto lPos = left.getCurrSelectedValuesPos();
            auto lValue = lValues[lPos];
            auto isLeftNull = leftNullMask[lPos];
            auto selectedValuesPos = right.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultNullMask[pos] = isLeftNull || rightNullMask[pos];
                if (!resultNullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lValue, rValues[pos]);
                }
            }
        } else if (right.isFlat()) {
            size = left.getNumSelectedValues();
            auto rPos = right.getCurrSelectedValuesPos();
            auto rValue = rValues[rPos];
            auto isRightNull = rightNullMask[rPos];
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultNullMask[pos] = leftNullMask[pos] || isRightNull;
                if (!resultNullMask[i]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lValues[pos], rValue);
                }
            }
        } else {
            size = left.getNumSelectedValues();
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultNullMask[pos] = leftNullMask[pos] || rightNullMask[pos];
                if (!resultNullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lValues[pos], rValues[pos]);
                }
            }
        }
        result.owner->numSelectedValues = size;
    }

    // A (left operand type), B (right operand type), R (result type)
    template<class FUNC = function<uint8_t(uint8_t, uint8_t, bool, bool)>>
    static void executeBoolOp(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = left.getValues();
        auto rValues = right.getValues();
        auto resultValues = result.getValues();
        auto leftNullMask = left.getNullMask();
        auto rightNullMask = right.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        if (left.isFlat() && right.isFlat()) {
            auto lPos = left.getCurrSelectedValuesPos();
            auto rPos = right.getCurrSelectedValuesPos();
            auto resPos = result.getCurrSelectedValuesPos();
            resultValues[resPos] = FUNC::operation(
                lValues[lPos], rValues[rPos], leftNullMask[lPos], rightNullMask[rPos]);
            resultNullMask[resPos] = resultValues[resPos] == NULL_BOOL;
            size = 1;
        } else if (left.isFlat()) {
            size = right.getNumSelectedValues();
            auto lPos = left.getCurrSelectedValuesPos();
            auto lValue = lValues[lPos];
            auto isLeftNull = leftNullMask[lPos];
            auto selectedValuesPos = right.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[pos] =
                    FUNC::operation(lValue, rValues[pos], isLeftNull, rightNullMask[pos]);
                resultNullMask[pos] = resultValues[pos] == NULL_BOOL;
            }
        } else if (right.isFlat()) {
            size = left.getNumSelectedValues();
            auto rPos = right.getCurrSelectedValuesPos();
            auto rValue = rValues[rPos];
            auto isRightNull = rightNullMask[rPos];
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[pos] =
                    FUNC::operation(lValues[pos], rValue, leftNullMask[pos], isRightNull);
                resultNullMask[pos] = resultValues[pos] == NULL_BOOL;
            }
        } else {
            size = left.getNumSelectedValues();
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[pos] = FUNC::operation(
                    lValues[pos], rValues[pos], leftNullMask[pos], rightNullMask[pos]);
                resultNullMask[pos] = resultValues[pos] == NULL_BOOL;
            }
        }
        result.owner->numSelectedValues = size;
    }

    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static void executeOnNodeIDs(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto resultValues = result.getValues();
        auto leftNullMask = left.getNullMask();
        auto rightNullMask = right.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        nodeID_t lNodeID{}, rNodeID{};
        if (left.isFlat() && right.isFlat()) {
            left.readNodeOffsetAndLabel(left.getCurrSelectedValuesPos(), lNodeID);
            right.readNodeOffsetAndLabel(right.getCurrSelectedValuesPos(), rNodeID);
            auto resPos = result.getCurrSelectedValuesPos();
            resultNullMask[resPos] =
                leftNullMask[left.getCurrPos()] || rightNullMask[right.getCurrPos()];
            if (!resultNullMask[resPos]) { // not NULL.
                resultValues[resPos] = FUNC::operation(lNodeID, rNodeID);
            }
            size = 1;
        } else if (left.isFlat()) {
            size = right.size();
            auto lNullMask = leftNullMask[left.getCurrPos()];
            auto selectedValuesPos = right.getSelectedValuesPos();
            left.readNodeOffsetAndLabel(left.getCurrSelectedValuesPos(), lNodeID);
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                right.readNodeOffsetAndLabel(pos, rNodeID);
                resultNullMask[i] = lNullMask || rightNullMask[right.getCurrPos()];
                if (!resultNullMask[i]) { // not NULL.
                    resultValues[i] = FUNC::operation(lNodeID, rNodeID);
                }
            }
        } else if (right.isFlat()) {
            size = left.size();
            auto rNullMask = leftNullMask[left.getCurrPos()];
            auto selectedValuesPos = left.getSelectedValuesPos();
            right.readNodeOffsetAndLabel(right.getCurrSelectedValuesPos(), rNodeID);
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                left.readNodeOffsetAndLabel(pos, lNodeID);
                resultNullMask[pos] = leftNullMask[left.getCurrPos()] || rNullMask;
                if (!resultNullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lNodeID, rNodeID);
                }
            }
            result.owner->numSelectedValues = size;
        } else {
            size = left.size();
            auto selectedValuesPos = left.getSelectedValuesPos();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                left.readNodeOffsetAndLabel(pos, lNodeID);
                right.readNodeOffsetAndLabel(pos, rNodeID);
                resultNullMask[pos] =
                    leftNullMask[left.getCurrPos()] || rightNullMask[right.getCurrPos()];
                if (!resultNullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(lNodeID, rNodeID);
                }
            }
        }
        result.owner->numSelectedValues = size;
    }
};

} // namespace common
} // namespace graphflow
