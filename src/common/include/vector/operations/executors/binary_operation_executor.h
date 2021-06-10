#pragma once

#include <functional>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {

    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>>
    static void executeArithmeticOps(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        auto resultValues = (R*)result.values;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[lPos] || right.nullMask[rPos];
            if (!result.nullMask[resPos]) {
                FUNC::operation(lValues[lPos], rValues[rPos], resultValues[resPos]);
            }
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto lValue = lValues[lPos];
            auto isLeftNull = left.nullMask[lPos];
            // right and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < right.state->size; i++) {
                auto pos = right.state->selectedValuesPos[i];
                result.nullMask[pos] = isLeftNull || right.nullMask[pos];
                if (!result.nullMask[pos]) {
                    FUNC::operation(lValue, rValues[pos], resultValues[pos]);
                }
            }
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto rValue = rValues[rPos];
            auto isRightNull = right.nullMask[rPos];
            // left and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < left.state->size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || isRightNull;
                if (!result.nullMask[i]) {
                    FUNC::operation(lValues[pos], rValue, resultValues[pos]);
                }
            }
        } else {
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < result.state->size; i++) {
                auto pos = result.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || right.nullMask[pos];
                if (!result.nullMask[pos]) {
                    FUNC::operation(lValues[pos], rValues[pos], resultValues[pos]);
                }
            }
        }
    }

    template<class A, class B, class FUNC = function<void(A, B)>>
    static void executeComparisonOps(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[lPos] || right.nullMask[rPos];
            if (!result.nullMask[resPos]) {
                result.values[resPos] = FUNC::operation(lValues[lPos], rValues[rPos]);
            }
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            auto lValue = lValues[lPos];
            auto isLeftNull = left.nullMask[lPos];
            // right and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < right.state->size; i++) {
                auto pos = right.state->selectedValuesPos[i];
                result.nullMask[pos] = isLeftNull || right.nullMask[pos];
                if (!result.nullMask[pos]) {
                    result.values[pos] = FUNC::operation(lValue, rValues[pos]);
                }
            }
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getCurrSelectedValuesPos();
            auto rValue = rValues[rPos];
            auto isRightNull = right.nullMask[rPos];
            // left and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < left.state->size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || isRightNull;
                if (!result.nullMask[i]) {
                    result.values[pos] = FUNC::operation(lValues[pos], rValue);
                }
            }
        } else {
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < left.state->size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                result.nullMask[pos] = left.nullMask[pos] || right.nullMask[pos];
                if (!result.nullMask[pos]) {
                    result.values[pos] = FUNC::operation(lValues[pos], rValues[pos]);
                }
            }
        }
    }

    template<class FUNC = function<uint8_t(uint8_t, uint8_t, bool, bool)>>
    static void executeBoolOps(ValueVector& left, ValueVector& right, ValueVector& result) {
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
            auto isFlatNull = flatVector.nullMask[flatVector.state->getCurrSelectedValuesPos()];
            // unflat and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < unflatVector.state->size; i++) {
                pos = right.state->selectedValuesPos[i];
                result.values[pos] = FUNC::operation(
                    flatValue, unflatVector.values[pos], isFlatNull, unflatVector.nullMask[pos]);
                result.nullMask[pos] = result.values[pos] == NULL_BOOL;
            }
        } else {
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < result.state->size; i++) {
                auto pos = result.state->selectedValuesPos[i];
                result.values[pos] = FUNC::operation(
                    left.values[pos], right.values[pos], left.nullMask[pos], right.nullMask[pos]);
                result.nullMask[pos] = result.values[pos] == NULL_BOOL;
            }
        }
    }

    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static void executeOnNodeIDs(ValueVector& left, ValueVector& right, ValueVector& result) {
        nodeID_t nodeID, otherNodeID;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getCurrSelectedValuesPos();
            left.readNodeOffsetAndLabel(lPos, nodeID);
            auto rPos = right.state->getCurrSelectedValuesPos();
            right.readNodeOffsetAndLabel(rPos, otherNodeID);
            auto resPos = result.state->getCurrSelectedValuesPos();
            result.nullMask[resPos] = left.nullMask[lPos] || right.nullMask[rPos];
            if (!result.nullMask[resPos]) {
                result.values[resPos] = FUNC::operation(nodeID, otherNodeID);
            }
        } else if (left.state->isFlat() || right.state->isFlat()) {
            auto isLeftFlat = left.state->isFlat();
            auto& flatVector = isLeftFlat ? left : right;
            auto& unflatVector = !isLeftFlat ? left : right;
            auto pos = flatVector.state->getCurrSelectedValuesPos();
            auto isFlatNull = flatVector.nullMask[pos];
            flatVector.readNodeOffsetAndLabel(pos, nodeID);
            // unflat and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < unflatVector.state->size; i++) {
                pos = unflatVector.state->selectedValuesPos[i];
                unflatVector.readNodeOffsetAndLabel(pos, otherNodeID);
                result.nullMask[i] = isFlatNull || unflatVector.nullMask[pos];
                if (!result.nullMask[i]) {
                    result.values[i] = isLeftFlat ? FUNC::operation(nodeID, otherNodeID) :
                                                    FUNC::operation(otherNodeID, nodeID);
                }
            }
        } else {
            // right, left, and result vectors share the same selectedValuesPos.
            for (uint64_t i = 0; i < left.state->size; i++) {
                auto pos = left.state->selectedValuesPos[i];
                left.readNodeOffsetAndLabel(pos, nodeID);
                right.readNodeOffsetAndLabel(pos, otherNodeID);
                result.nullMask[pos] = left.nullMask[pos] || right.nullMask[pos];
                if (!result.nullMask[pos]) {
                    result.values[pos] = FUNC::operation(nodeID, otherNodeID);
                }
            }
        }
    }
};

} // namespace common
} // namespace graphflow
