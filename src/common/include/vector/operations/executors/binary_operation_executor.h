#pragma once

#include <functional>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {
    static void setUnstructuredString(Value& value, const string& str, ValueVector& vec) {
        vec.allocateStringOverflowSpace(value.val.strVal, str.length());
        value.val.strVal.set(str);
        value.dataType = STRING;
    }

    static void allocateUnstructuredString(Value& lValue, Value& rValue, Value& resVal,
        ValueVector& lVec, ValueVector& rVec, ValueVector& resVec) {
        assert(lValue.dataType == STRING || rValue.dataType == STRING);
        if (lValue.dataType != STRING) {
            setUnstructuredString(lValue, lValue.toString(), lVec);
        }
        if (rValue.dataType != STRING) {
            setUnstructuredString(rValue, rValue.toString(), rVec);
        }
        resVec.allocateStringOverflowSpace(
            resVal.val.strVal, lValue.val.strVal.len + rValue.val.strVal.len);
    }

    template<typename A, typename B, typename R>
    static void allocateStringIfNecessary(A& lValue, B& rValue, R& resultVal, ValueVector& lVec,
        ValueVector& rVec, ValueVector& resultVec) {
        assert((is_same<R, Value>::value) || (is_same<R, gf_string_t>::value));
        if constexpr ((is_same<R, Value>::value)) {
            if (lValue.dataType == STRING || rValue.dataType == STRING) {
                allocateUnstructuredString(lValue, rValue, resultVal, lVec, rVec, resultVec);
            }
        } else {
            resultVec.allocateStringOverflowSpace(resultVal, lValue.len + rValue.len);
        }
    }

    template<typename A, typename B, typename R, typename FUNC = function<void(A&, B&, R&)>>
    static void executeOnTuple(ValueVector& left, ValueVector& right, ValueVector& result,
        uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        auto resValues = (R*)result.values;
        if constexpr ((is_same<A, nodeID_t>::value) && (is_same<B, nodeID_t>::value)) {
            nodeID_t lNodeID, rNodeID;
            left.readNodeID(lPos, lNodeID);
            right.readNodeID(rPos, rNodeID);
            FUNC::operation(lNodeID, rNodeID, resValues[resPos], (bool)left.isNull(lPos),
                (bool)right.isNull(rPos));
        } else {
            if constexpr ((is_same<R, gf_string_t>::value) || (is_same<R, Value>::value)) {
                allocateStringIfNecessary<A, B, R>(
                    lValues[lPos], rValues[rPos], resValues[resPos], left, right, result);
            }
            FUNC::operation(lValues[lPos], rValues[rPos], resValues[resPos],
                (bool)left.isNull(lPos), (bool)right.isNull(rPos));
        }
    }

    template<typename A, typename B, typename R, typename FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL>
    static void executeForLeftAndRightAreFlat(
        ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        if constexpr (SKIP_NULL) {
            if (!result.isNull(resPos)) {
                executeOnTuple<A, B, R, FUNC>(left, right, result, lPos, rPos, resPos);
            }
            result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
        } else {
            executeOnTuple<A, B, R, FUNC>(left, right, result, lPos, rPos, resPos);
            result.setNull(resPos, result.values[resPos] == NULL_BOOL);
        }
    }

    template<typename A, typename B, typename R, typename FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL>
    static void executeForLeftOrRightIsFlat(
        ValueVector& left, ValueVector& right, ValueVector& result) {
        auto& flatVec = left.state->isFlat() ? left : right;
        auto& unFlatVec = left.state->isFlat() ? right : left;
        auto flatPos = flatVec.state->getPositionOfCurrIdx();
        auto isFlatNull = flatVec.isNull(flatPos);
        auto isLeftFlat = left.state->isFlat();
        if constexpr (SKIP_NULL) {
            if (isFlatNull) {
                unFlatVec.setAllNull();
            } else if (unFlatVec.hasNoNullsGuarantee()) {
                if (unFlatVec.state->isUnfiltered()) {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        executeOnTuple<A, B, R, FUNC>(left, right, result, isLeftFlat ? flatPos : i,
                            isLeftFlat ? i : flatPos, i);
                    }
                } else {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        auto unFlatPos = unFlatVec.state->selectedPositions[i];
                        executeOnTuple<A, B, R, FUNC>(left, right, result,
                            isLeftFlat ? flatPos : unFlatPos, isLeftFlat ? unFlatPos : flatPos,
                            unFlatPos);
                    }
                }
            } else {
                if (unFlatVec.state->isUnfiltered()) {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        result.setNull(i, unFlatVec.isNull(i)); // isFlatNull is always false.
                        if (!result.isNull(i)) {
                            executeOnTuple<A, B, R, FUNC>(left, right, result,
                                isLeftFlat ? flatPos : i, isLeftFlat ? i : flatPos, i);
                        }
                    }
                } else {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        auto unFlatPos = unFlatVec.state->selectedPositions[i];
                        result.setNull(
                            unFlatPos, unFlatVec.isNull(unFlatPos)); // isFlatNull is always false.
                        if (!result.isNull(unFlatPos)) {
                            executeOnTuple<A, B, R, FUNC>(left, right, result,
                                isLeftFlat ? flatPos : unFlatPos, isLeftFlat ? unFlatPos : flatPos,
                                unFlatPos);
                        }
                    }
                }
            }
        } else {
            if (unFlatVec.state->isUnfiltered()) {
                for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                    result.setNull(i, unFlatVec.isNull(i)); // isFlatNull is always false.
                    executeOnTuple<A, B, R, FUNC>(
                        left, right, result, isLeftFlat ? flatPos : i, isLeftFlat ? i : flatPos, i);
                }
            } else {
                for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                    auto unFlatPos = unFlatVec.state->selectedPositions[i];
                    result.setNull(
                        unFlatPos, unFlatVec.isNull(unFlatPos)); // isFlatNull is always false.
                    executeOnTuple<A, B, R, FUNC>(left, right, result,
                        isLeftFlat ? flatPos : unFlatPos, isLeftFlat ? unFlatPos : flatPos,
                        unFlatPos);
                }
            }
        }
    }

    template<typename A, typename B, typename R, typename FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL>
    static void executeForLeftAndRightAreUnFlat(
        ValueVector& left, ValueVector& right, ValueVector& result) {
        // right, left, and result vectors share the same selectedPositions.
        if constexpr (SKIP_NULL) {
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                if (result.state->isUnfiltered()) {
                    for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                        executeOnTuple<A, B, R, FUNC>(left, right, result, i, i, i);
                    }
                } else {
                    for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                        auto pos = result.state->selectedPositions[i];
                        executeOnTuple<A, B, R, FUNC>(left, right, result, pos, pos, pos);
                    }
                }
            } else {
                if (result.state->isUnfiltered()) {
                    for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                        result.setNull(i, left.isNull(i) || right.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnTuple<A, B, R, FUNC>(left, right, result, i, i, i);
                        }
                    }
                } else {
                    for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                        auto pos = result.state->selectedPositions[i];
                        result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnTuple<A, B, R, FUNC>(left, right, result, pos, pos, pos);
                        }
                    }
                }
            }
        } else {
            if (result.state->isUnfiltered()) {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    result.setNull(i, result.values[i] == NULL_BOOL);
                    executeOnTuple<A, B, R, FUNC>(left, right, result, i, i, i);
                }
            } else {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    auto pos = result.state->selectedPositions[i];
                    result.setNull(pos, result.values[pos] == NULL_BOOL);
                    executeOnTuple<A, B, R, FUNC>(left, right, result, pos, pos, pos);
                }
            }
        }
    }

    template<typename A, typename B, typename R, typename FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL = true>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        // SKIP_NULL is set to false ONLY when the FUNC is boolean operations.
        assert(SKIP_NULL || (!SKIP_NULL && (is_same<A, uint8_t>::value) &&
                                (is_same<B, uint8_t>::value) && (is_same<R, uint8_t>::value)));
        if (left.state->isFlat() && right.state->isFlat()) {
            executeForLeftAndRightAreFlat<A, B, R, FUNC, SKIP_NULL>(left, right, result);
        } else if (left.state->isFlat() || right.state->isFlat()) {
            executeForLeftOrRightIsFlat<A, B, R, FUNC, SKIP_NULL>(left, right, result);
        } else {
            executeForLeftAndRightAreUnFlat<A, B, R, FUNC, SKIP_NULL>(left, right, result);
        }
    }

    // ARITHMETIC (ADD, SUBSTRACT, MULTIPLY, DIVIDE, MODULO, POWER), COMPARISON (GT, GTE, LT, LTE,
    // EQ, NEQ), CONJUNCTION (AND, OR, XOR)
    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto rPos = right.state->getPositionOfCurrIdx();
            uint64_t numSelectedValues = 0;
            auto isNull = left.isNull(lPos) || right.isNull(rPos);
            if (!isNull) {
                uint8_t resultValue;
                FUNC::operation(lValues[lPos], rValues[rPos], resultValue, (bool)left.isNull(lPos),
                    (bool)right.isNull(rPos));
                numSelectedValues += resultValue == TRUE;
            }
            return numSelectedValues;
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto& lValue = lValues[lPos];
            auto isLeftNull = left.isNull(lPos);
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            // right and result vectors share the same selectedPositions.
            if (!isLeftNull && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    FUNC::operation(lValue, rValues[rPos], resultValue, (bool)isLeftNull,
                        (bool)right.isNull(rPos));
                    selectedPositions[numSelectedValues] = rPos;
                    numSelectedValues += resultValue;
                }
            } else if (isLeftNull) {
                // Do nothing: numSelectedValues = 0;
            } else {
                for (uint64_t i = 0; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    auto isNull = isLeftNull || right.isNull(rPos);
                    if (!isNull) {
                        FUNC::operation(lValue, rValues[rPos], resultValue, (bool)isLeftNull,
                            (bool)right.isNull(rPos));
                        selectedPositions[numSelectedValues] = rPos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto& rValue = rValues[rPos];
            auto isRightNull = right.isNull(rPos);
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            // left and result vectors share the same selectedPositions.
            if (!isRightNull && left.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    FUNC::operation(lValues[lPos], rValue, resultValue, (bool)left.isNull(lPos),
                        (bool)isRightNull);
                    selectedPositions[numSelectedValues] = lPos;
                    numSelectedValues += resultValue;
                }
            } else if (isRightNull) {
                // Do nothing: numSelectedValues = 0;
            } else {
                // left and result vectors share the same selectedPositions.
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    bool isNull = left.isNull(lPos) || isRightNull;
                    if (!isNull) {
                        FUNC::operation(lValues[lPos], rValue, resultValue, (bool)left.isNull(lPos),
                            (bool)isRightNull);
                        selectedPositions[numSelectedValues] = lPos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        } else {
            // right, left, and result vectors share the same selectedPositions.
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    FUNC::operation(lValues[pos], rValues[pos], resultValue, (bool)left.isNull(pos),
                        (bool)right.isNull(pos));
                    selectedPositions[numSelectedValues] = pos;
                    numSelectedValues += resultValue;
                }
            } else {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    auto isNull = left.isNull(pos) || right.isNull(pos);
                    if (!isNull) {
                        FUNC::operation(lValues[pos], rValues[pos], resultValue,
                            (bool)left.isNull(pos), (bool)right.isNull(pos));
                        selectedPositions[numSelectedValues] = pos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        }
    }

    // AND, OR, XOR
    template<class FUNC = function<uint8_t(uint8_t, uint8_t, bool, bool)>>
    static uint64_t selectBooleanOps(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto rPos = right.state->getPositionOfCurrIdx();
            uint8_t resultValue;
            FUNC::operation(left.values[lPos], right.values[rPos], resultValue,
                (bool)left.isNull(lPos), (bool)right.isNull(rPos));
            return resultValue == TRUE;
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto& lValue = left.values[lPos];
            auto isLeftNull = left.isNull(lPos);
            // unFlat and result vectors share the same selectedPositions.
            uint64_t numSelectedValues = 0;
            uint8_t resultValue;
            for (auto i = 0u; i < right.state->selectedSize; i++) {
                auto rPos = right.state->selectedPositions[i];
                FUNC::operation(lValue, right.values[rPos], resultValue, (bool)isLeftNull,
                    (bool)right.isNull(rPos));
                selectedPositions[numSelectedValues] = rPos;
                numSelectedValues += (resultValue == TRUE);
            }
            return numSelectedValues;
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto& rValue = right.values[rPos];
            auto isRightNull = right.isNull(rPos);
            // unFlat and result vectors share the same selectedPositions.
            uint64_t numSelectedValues = 0;
            uint8_t resultValue;
            for (auto i = 0u; i < left.state->selectedSize; i++) {
                auto lPos = right.state->selectedPositions[i];
                FUNC::operation(left.values[lPos], rValue, resultValue, (bool)left.isNull(lPos),
                    (bool)isRightNull);
                selectedPositions[numSelectedValues] = lPos;
                numSelectedValues += (resultValue == TRUE);
            }
            return numSelectedValues;
        } else {
            // right, left, and result vectors share the same selectedPositions.
            uint64_t numSelectedValues = 0;
            uint8_t resultValue;
            for (auto i = 0u; i < left.state->selectedSize; i++) {
                auto pos = left.state->selectedPositions[i];
                FUNC::operation(left.values[pos], right.values[pos], resultValue,
                    (bool)left.isNull(pos), (bool)right.isNull(pos));
                selectedPositions[numSelectedValues] = pos;
                numSelectedValues += (resultValue == TRUE);
            }
            return numSelectedValues;
        }
    }

    // Specialized for nodeID_t data type
    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static uint64_t selectNodeIDOps(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        nodeID_t leftNodeID, rightNodeID;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            left.readNodeID(lPos, leftNodeID);
            auto rPos = right.state->getPositionOfCurrIdx();
            right.readNodeID(rPos, rightNodeID);
            auto isNull = left.isNull(lPos) || right.isNull(rPos);
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            if (!isNull) {
                FUNC::operation(leftNodeID, rightNodeID, resultValue, (bool)left.isNull(lPos),
                    (bool)right.isNull(rPos));
                numSelectedValues += resultValue == TRUE;
            }
            return numSelectedValues;
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto isLeftNull = left.isNull(lPos);
            left.readNodeID(lPos, leftNodeID);
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            // unFlat and result vectors share the same selectedPositions.
            if (!isLeftNull && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    right.readNodeID(rPos, rightNodeID);
                    FUNC::operation(leftNodeID, rightNodeID, resultValue, (bool)isLeftNull,
                        (bool)right.isNull(rPos));
                    selectedPositions[numSelectedValues] = rPos;
                    numSelectedValues += resultValue;
                }
            } else if (isLeftNull) {
                // Do nothing: numSelectedValues = 0;
            } else {
                for (auto i = 0u; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    right.readNodeID(rPos, rightNodeID);
                    auto isNull = isLeftNull || right.isNull(rPos);
                    if (!isNull) {
                        FUNC::operation(leftNodeID, rightNodeID, resultValue, (bool)isLeftNull,
                            (bool)right.isNull(rPos));
                        selectedPositions[numSelectedValues] = rPos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto isRightNull = right.isNull(rPos);
            right.readNodeID(rPos, rightNodeID);
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            // unFlat and result vectors share the same selectedPositions.
            if (!isRightNull && left.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    left.readNodeID(lPos, leftNodeID);
                    FUNC::operation(leftNodeID, rightNodeID, resultValue, (bool)left.isNull(lPos),
                        (bool)isRightNull);
                    selectedPositions[numSelectedValues] = lPos;
                    numSelectedValues += resultValue;
                }
            } else if (isRightNull) {
                // Do nothing: numSelectedValues = 0;
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    left.readNodeID(lPos, leftNodeID);
                    auto isNull = isRightNull || left.isNull(lPos);
                    if (!isNull) {
                        FUNC::operation(leftNodeID, rightNodeID, resultValue,
                            (bool)left.isNull(lPos), (bool)isRightNull);
                        selectedPositions[numSelectedValues] = lPos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        } else {
            // right, left, and result vectors share the same selectedPositions.
            uint64_t numSelectedValues = 0;
            uint8_t resultValue = 0;
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    left.readNodeID(pos, leftNodeID);
                    right.readNodeID(pos, rightNodeID);
                    FUNC::operation(leftNodeID, rightNodeID, resultValue, (bool)left.isNull(pos),
                        (bool)right.isNull(pos));
                    selectedPositions[numSelectedValues] = pos;
                    numSelectedValues += resultValue;
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    left.readNodeID(pos, leftNodeID);
                    right.readNodeID(pos, rightNodeID);
                    auto isNull = left.isNull(pos) || right.isNull(pos);
                    if (!isNull) {
                        FUNC::operation(leftNodeID, rightNodeID, resultValue,
                            (bool)left.isNull(pos), (bool)right.isNull(pos));
                        selectedPositions[numSelectedValues] = pos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        }
    }
}; // namespace common

} // namespace common
} // namespace graphflow
