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
        auto resValues = (R*)result.values;
        if constexpr ((is_same<A, nodeID_t>::value) && (is_same<B, nodeID_t>::value)) {
            nodeID_t lNodeID, rNodeID;
            left.readNodeID(lPos, lNodeID);
            right.readNodeID(rPos, rNodeID);
            FUNC::operation(lNodeID, rNodeID, resValues[resPos], (bool)left.isNull(lPos),
                (bool)right.isNull(rPos));
        } else {
            auto lValues = (A*)left.values;
            auto rValues = (B*)right.values;
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
                        result.setNull(unFlatPos,
                            unFlatVec.isNull(unFlatPos)); // isFlatNull is always false.
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
                    result.setNull(unFlatPos,
                        unFlatVec.isNull(unFlatPos)); // isFlatNull is always false.
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

    // By default, UPDATE_SELECTED_POSITIONS is set to true. It is set to false only when both left
    // and right are flat, in that case, we are not supposed to update the selectedPositions.
    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>,
        bool UPDATE_SELECTED_POSITIONS = true>
    static void selectOnTuple(ValueVector& left, ValueVector& right, uint64_t lPos, uint64_t rPos,
        uint64_t resPos, uint64_t& numSelectedValues, sel_t* selectedPositions) {
        uint8_t resultValue = 0;
        if constexpr ((is_same<A, nodeID_t>::value) && (is_same<B, nodeID_t>::value)) {
            nodeID_t lNodeID{}, rNodeID{};
            left.readNodeID(lPos, lNodeID);
            right.readNodeID(rPos, rNodeID);
            FUNC::operation(
                lNodeID, rNodeID, resultValue, (bool)left.isNull(lPos), (bool)right.isNull(rPos));
        } else {
            auto lValues = (A*)left.values;
            auto rValues = (B*)right.values;
            FUNC::operation(lValues[lPos], rValues[rPos], resultValue, (bool)left.isNull(lPos),
                (bool)right.isNull(rPos));
        }
        if constexpr (UPDATE_SELECTED_POSITIONS) {
            assert(selectedPositions);
            selectedPositions[numSelectedValues] = resPos;
        }
        numSelectedValues += resultValue == TRUE;
    }

    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>, bool SKIP_NULL>
    static uint64_t selectForLeftAndRightAreFlat(ValueVector& left, ValueVector& right) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        if constexpr (SKIP_NULL) {
            if (!left.isNull(lPos) && !right.isNull(rPos)) {
                selectOnTuple<A, B, R, FUNC, false /* UPDATE_SELECTED_POSITIONS */>(
                    left, right, lPos, rPos, 0 /* resPos */, numSelectedValues, nullptr);
            }
        } else {
            selectOnTuple<A, B, R, FUNC, false /* UPDATE_SELECTED_POSITIONS */>(
                left, right, lPos, rPos, 0 /* resPos */, numSelectedValues, nullptr);
        }
        return numSelectedValues;
    }

    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>, bool SKIP_NULL>
    static uint64_t selectForLeftOrRightIsFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto& flatVec = left.state->isFlat() ? left : right;
        auto& unFlatVec = left.state->isFlat() ? right : left;
        auto flatPos = flatVec.state->getPositionOfCurrIdx();
        auto isFlatNull = flatVec.isNull(flatPos);
        auto isLeftFlat = left.state->isFlat();
        uint64_t numSelectedValues = 0;
        if constexpr (SKIP_NULL) {
            if (isFlatNull) {
                // Do nothing here: numSelectedValues = 0;
            } else if (unFlatVec.hasNoNullsGuarantee()) {
                // right and result vectors share the same selectedPositions.
                if (unFlatVec.state->isUnfiltered()) {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        selectOnTuple<A, B, R, FUNC>(left, right, isLeftFlat ? flatPos : i,
                            isLeftFlat ? i : flatPos, i, numSelectedValues, selectedPositions);
                    }
                } else {
                    for (auto i = 0u; i < unFlatVec.state->selectedSize; i++) {
                        auto unFlatPos = unFlatVec.state->selectedPositions[i];
                        selectOnTuple<A, B, R, FUNC>(left, right, isLeftFlat ? flatPos : unFlatPos,
                            isLeftFlat ? unFlatPos : flatPos, unFlatPos, numSelectedValues,
                            selectedPositions);
                    }
                }
            } else {
                if (unFlatVec.state->isUnfiltered()) {
                    for (uint64_t i = 0; i < unFlatVec.state->selectedSize; i++) {
                        if (!unFlatVec.isNull(i)) {
                            selectOnTuple<A, B, R, FUNC>(left, right, isLeftFlat ? flatPos : i,
                                isLeftFlat ? i : flatPos, i, numSelectedValues, selectedPositions);
                        }
                    }
                } else {
                    for (uint64_t i = 0; i < unFlatVec.state->selectedSize; i++) {
                        auto unFlatPos = unFlatVec.state->selectedPositions[i];
                        if (!unFlatVec.isNull(unFlatPos)) {
                            selectOnTuple<A, B, R, FUNC>(left, right,
                                isLeftFlat ? flatPos : unFlatPos, isLeftFlat ? unFlatPos : flatPos,
                                unFlatPos, numSelectedValues, selectedPositions);
                        }
                    }
                }
            }
        } else {
            if (unFlatVec.state->isUnfiltered()) {
                for (uint64_t i = 0; i < unFlatVec.state->selectedSize; i++) {
                    if (!unFlatVec.isNull(i)) {
                        selectOnTuple<A, B, R, FUNC>(left, right, isLeftFlat ? flatPos : i,
                            isLeftFlat ? i : flatPos, i, numSelectedValues, selectedPositions);
                    }
                }
            } else {
                for (uint64_t i = 0; i < unFlatVec.state->selectedSize; i++) {
                    auto unFlatPos = unFlatVec.state->selectedPositions[i];
                    if (!unFlatVec.isNull(unFlatPos)) {
                        selectOnTuple<A, B, R, FUNC>(left, right, isLeftFlat ? flatPos : unFlatPos,
                            isLeftFlat ? unFlatPos : flatPos, unFlatPos, numSelectedValues,
                            selectedPositions);
                    }
                }
            }
        }
        return numSelectedValues;
    }

    // SKIP_NULL is set to false ONLY when the FUNC is boolean operations.
    // Right, left, and result vectors share the same selectedPositions.
    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL = true>
    static uint64_t selectForLeftAndRightAreUnFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        uint64_t numSelectedValues = 0;
        if constexpr (SKIP_NULL) {
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                if (left.state->isUnfiltered()) {
                    for (auto i = 0u; i < left.state->selectedSize; i++) {
                        selectOnTuple<A, B, R, FUNC>(
                            left, right, i, i, i, numSelectedValues, selectedPositions);
                    }
                } else {
                    for (auto i = 0u; i < left.state->selectedSize; i++) {
                        auto pos = left.state->selectedPositions[i];
                        selectOnTuple<A, B, R, FUNC>(
                            left, right, pos, pos, pos, numSelectedValues, selectedPositions);
                    }
                }
            } else {
                if (left.state->isUnfiltered()) {
                    for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                        auto isNull = left.isNull(i) || right.isNull(i);
                        if (!isNull) {
                            selectOnTuple<A, B, R, FUNC>(
                                left, right, i, i, i, numSelectedValues, selectedPositions);
                        }
                    }
                } else {
                    for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                        auto pos = left.state->selectedPositions[i];
                        auto isNull = left.isNull(pos) || right.isNull(pos);
                        if (!isNull) {
                            selectOnTuple<A, B, R, FUNC>(
                                left, right, pos, pos, pos, numSelectedValues, selectedPositions);
                        }
                    }
                }
            }
        } else {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    selectOnTuple<A, B, R, FUNC>(
                        left, right, i, i, i, numSelectedValues, selectedPositions);
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    selectOnTuple<A, B, R, FUNC>(
                        left, right, pos, pos, pos, numSelectedValues, selectedPositions);
                }
            }
        }
        return numSelectedValues;
    }

    // COMPARISON (GT, GTE, LT, LTE, EQ, NEQ), BOOLEAN (AND, OR, XOR)
    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>,
        bool SKIP_NULL = true>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectForLeftAndRightAreFlat<A, B, R, FUNC, SKIP_NULL>(left, right);
        } else if (left.state->isFlat() || right.state->isFlat()) {
            return selectForLeftOrRightIsFlat<A, B, R, FUNC, SKIP_NULL>(
                left, right, selectedPositions);
        } else {
            return selectForLeftAndRightAreUnFlat<A, B, R, FUNC, SKIP_NULL>(
                left, right, selectedPositions);
        }
    }
};

} // namespace common
} // namespace graphflow
