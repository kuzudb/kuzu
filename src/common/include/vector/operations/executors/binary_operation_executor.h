#pragma once

#include <functional>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {

struct BinaryOperationExecutor {

    template<class A, class B, class R, bool IS_STRUCTURED_STRING, bool IS_UNSTRUCTURED>
    static void allocateStringIfNecessary(A& lValue, B& rValue, R& resultVal, ValueVector& lVec,
        ValueVector& rVec, ValueVector& resultVec) {
        if constexpr (IS_STRUCTURED_STRING) {
            resultVec.allocateStringOverflowSpace(resultVal, lValue.len + rValue.len);
        } else if constexpr (IS_UNSTRUCTURED) {
            if (lValue.dataType == STRING || rValue.dataType == STRING) {
                prepareUnstructuredString(lValue, rValue, resultVal, lVec, rVec, resultVec);
            }
        }
    }

    static void prepareUnstructuredString(Value& lValue, Value& rValue, Value& resVal,
        ValueVector& lVec, ValueVector& rVec, ValueVector& resVec) {
        assert(lValue.dataType == STRING || rValue.dataType == STRING);
        if (lValue.dataType != STRING) {
            lVec.allocateStringOverflowSpace(lValue.val.strVal, lValue.toString().length());
            lValue.val.strVal.set(lValue.toString());
            lValue.dataType = STRING;
        } else {
            rVec.allocateStringOverflowSpace(rValue.val.strVal, rValue.toString().length());
            rValue.val.strVal.set(rValue.toString());
            rValue.dataType = STRING;
        }
        resVec.allocateStringOverflowSpace(
            resVal.val.strVal, lValue.val.strVal.len + rValue.val.strVal.len);
    }

    // ARITHMETIC (ADD, SUBSTRACT, MULTIPLY, DIVIDE, MODULO, POWER), COMPARISON (GT, GTE, LT, LTE,
    // EQ, NEQ), CONJUNCTION (AND, OR, XOR)
    template<class A, class B, class R, class FUNC = function<void(A&, B&, R&)>,
        bool IS_STRUCTURED_STRING, bool IS_UNSTRUCTURED>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        // Operands can't be structured string and unstructured value at the same time
        assert((IS_STRUCTURED_STRING && IS_UNSTRUCTURED) == false);
        auto lValues = (A*)left.values;
        auto rValues = (B*)right.values;
        auto resultValues = (R*)result.values;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto rPos = right.state->getPositionOfCurrIdx();
            auto resPos = result.state->getPositionOfCurrIdx();

            result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
            if (!result.isNull(resPos)) {
                if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                    allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING, IS_UNSTRUCTURED>(
                        lValues[lPos], rValues[rPos], resultValues[resPos], left, right, result);
                }
                FUNC::operation(lValues[lPos], rValues[rPos], resultValues[resPos]);
            }
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto& lValue = lValues[lPos];
            auto isLeftNull = left.isNull(lPos);
            // right and result vectors share the same selectedPositions.
            if (!isLeftNull && right.hasNoNullsGuarantee()) {
                for (uint64_t i = 0; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                        allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING, IS_UNSTRUCTURED>(
                            lValue, rValues[rPos], resultValues[rPos], left, right, result);
                    }
                    FUNC::operation(lValue, rValues[rPos], resultValues[rPos]);
                }
            } else if (isLeftNull) {
                right.setAllNull();
            } else {
                for (uint64_t i = 0; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    result.setNull(rPos, isLeftNull || right.isNull(rPos));
                    if (!result.isNull(rPos)) {
                        if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                            allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING,
                                IS_UNSTRUCTURED>(
                                lValue, rValues[rPos], resultValues[rPos], left, right, result);
                        }
                        FUNC::operation(lValue, rValues[rPos], resultValues[rPos]);
                    }
                }
            }
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto& rValue = rValues[rPos];
            auto isRightNull = right.isNull(rPos);
            // left and result vectors share the same selectedPositions.
            if (!isRightNull && left.hasNoNullsGuarantee()) {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                        allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING, IS_UNSTRUCTURED>(
                            lValues[lPos], rValue, resultValues[lPos], left, right, result);
                    }
                    FUNC::operation(lValues[lPos], rValue, resultValues[lPos]);
                }
            } else if (isRightNull) {
                left.setAllNull();
            } else {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    result.setNull(pos, left.isNull(pos) || isRightNull);
                    if (!result.isNull(i)) {
                        if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                            allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING,
                                IS_UNSTRUCTURED>(
                                lValues[pos], rValue, resultValues[pos], left, right, result);
                        }
                        FUNC::operation(lValues[pos], rValue, resultValues[pos]);
                    }
                }
            }
        } else {
            // right, left, and result vectors share the same selectedPositions.
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    auto pos = result.state->selectedPositions[i];
                    if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                        allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING, IS_UNSTRUCTURED>(
                            lValues[pos], rValues[pos], resultValues[pos], left, right, result);
                    }
                    FUNC::operation(lValues[pos], rValues[pos], resultValues[pos]);
                }
            } else {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    auto pos = result.state->selectedPositions[i];
                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                    if (!result.isNull(pos)) {
                        if constexpr (IS_STRUCTURED_STRING || IS_UNSTRUCTURED) {
                            allocateStringIfNecessary<A, B, R, IS_STRUCTURED_STRING,
                                IS_UNSTRUCTURED>(
                                lValues[pos], rValues[pos], resultValues[pos], left, right, result);
                        }
                        FUNC::operation(lValues[pos], rValues[pos], resultValues[pos]);
                    }
                }
            }
        }
    }

    // AND, OR, XOR
    template<class FUNC = function<uint8_t(uint8_t, uint8_t, bool, bool)>>
    static void executeBooleanOps(ValueVector& left, ValueVector& right, ValueVector& result) {
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto rPos = right.state->getPositionOfCurrIdx();
            auto resPos = result.state->getPositionOfCurrIdx();
            result.values[resPos] = FUNC::operation(
                left.values[lPos], right.values[rPos], left.isNull(lPos), right.isNull(rPos));
            result.setNull(resPos, result.values[resPos] == NULL_BOOL);
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto& leftValue = left.values[lPos];
            auto isLeftNull = left.isNull(lPos);
            // right and result vectors share the same selectedPositions.
            for (auto i = 0u; i < right.state->selectedSize; i++) {
                auto rPos = right.state->selectedPositions[i];
                result.values[rPos] =
                    FUNC::operation(leftValue, right.values[rPos], isLeftNull, right.isNull(rPos));
                result.setNull(rPos, result.values[rPos] == NULL_BOOL);
            }
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto& rightValue = right.values[rPos];
            auto isRightNull = right.isNull(rPos);
            // left and result vectors share the same selectedPositions.
            for (auto i = 0u; i < left.state->selectedSize; i++) {
                auto lPos = left.state->selectedPositions[i];
                result.values[lPos] =
                    FUNC::operation(left.values[lPos], rightValue, left.isNull(lPos), isRightNull);
                result.setNull(lPos, result.values[lPos] == NULL_BOOL);
            }
        } else {
            // right, left, and result vectors share the same selectedPositions.
            for (auto i = 0u; i < result.state->selectedSize; i++) {
                auto pos = result.state->selectedPositions[i];
                result.values[pos] = FUNC::operation(
                    left.values[pos], right.values[pos], left.isNull(pos), right.isNull(pos));
                result.setNull(pos, result.values[pos] == NULL_BOOL);
            }
        }
    }

    // Specialized for nodeID_t data type: EQ, NE, GT, GTE, LT, LTE
    template<class FUNC = std::function<uint8_t(nodeID_t, nodeID_t)>>
    static void executeNodeIDOps(ValueVector& left, ValueVector& right, ValueVector& result) {
        nodeID_t leftNodeID, rightNodeID;
        if (left.state->isFlat() && right.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            left.readNodeID(lPos, leftNodeID);
            auto rPos = right.state->getPositionOfCurrIdx();
            right.readNodeID(rPos, rightNodeID);
            auto resPos = result.state->getPositionOfCurrIdx();
            result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
            if (!result.isNull(resPos)) {
                FUNC::operation(leftNodeID, rightNodeID, result.values[resPos]);
            }
        } else if (left.state->isFlat()) {
            auto lPos = left.state->getPositionOfCurrIdx();
            auto isLeftNull = left.isNull(lPos);
            left.readNodeID(lPos, leftNodeID);
            // unFlat and result vectors share the same selectedPositions.
            if (!isLeftNull && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    right.readNodeID(rPos, rightNodeID);
                    FUNC::operation(leftNodeID, rightNodeID, result.values[rPos]);
                }
            } else if (isLeftNull) {
                right.setAllNull();
            } else {
                for (auto i = 0u; i < right.state->selectedSize; i++) {
                    auto rPos = right.state->selectedPositions[i];
                    right.readNodeID(rPos, rightNodeID);
                    result.setNull(rPos, isLeftNull || right.isNull(rPos));
                    if (!result.isNull(rPos)) {
                        FUNC::operation(leftNodeID, rightNodeID, result.values[rPos]);
                    }
                }
            }
        } else if (right.state->isFlat()) {
            auto rPos = right.state->getPositionOfCurrIdx();
            auto isRightNull = right.isNull(rPos);
            right.readNodeID(rPos, rightNodeID);
            // unFlat and result vectors share the same selectedPositions.
            if (!isRightNull && left.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    left.readNodeID(lPos, leftNodeID);
                    FUNC::operation(leftNodeID, rightNodeID, result.values[lPos]);
                }
            } else if (isRightNull) {
                left.setAllNull();
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto lPos = left.state->selectedPositions[i];
                    left.readNodeID(lPos, leftNodeID);
                    result.setNull(lPos, isRightNull || left.isNull(lPos));
                    if (!result.isNull(lPos)) {
                        FUNC::operation(leftNodeID, rightNodeID, result.values[lPos]);
                    }
                }
            }
        } else {
            // right, left, and result vectors share the same selectedPositions.
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    left.readNodeID(pos, leftNodeID);
                    right.readNodeID(pos, rightNodeID);
                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                    FUNC::operation(leftNodeID, rightNodeID, result.values[pos]);
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    left.readNodeID(pos, leftNodeID);
                    right.readNodeID(pos, rightNodeID);
                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                    if (!result.isNull(pos)) {
                        FUNC::operation(leftNodeID, rightNodeID, result.values[pos]);
                    }
                }
            }
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
                FUNC::operation(lValues[lPos], rValues[rPos], resultValue);
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
                    FUNC::operation(lValue, rValues[rPos], resultValue);
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
                        FUNC::operation(lValue, rValues[rPos], resultValue);
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
                    FUNC::operation(lValues[lPos], rValue, resultValue);
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
                        FUNC::operation(lValues[lPos], rValue, resultValue);
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
                    FUNC::operation(lValues[pos], rValues[pos], resultValue);
                    selectedPositions[numSelectedValues] = pos;
                    numSelectedValues += resultValue;
                }
            } else {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    auto isNull = left.isNull(pos) || right.isNull(pos);
                    if (!isNull) {
                        FUNC::operation(lValues[pos], rValues[pos], resultValue);
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
            auto resultValue = FUNC::operation(
                left.values[lPos], right.values[rPos], left.isNull(lPos), right.isNull(rPos));
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
                resultValue =
                    FUNC::operation(lValue, right.values[rPos], isLeftNull, right.isNull(rPos));
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
                resultValue =
                    FUNC::operation(left.values[lPos], rValue, left.isNull(lPos), isRightNull);
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
                resultValue = FUNC::operation(
                    left.values[pos], right.values[pos], left.isNull(pos), right.isNull(pos));
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
                FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                    FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                        FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                    FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                        FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                    FUNC::operation(leftNodeID, rightNodeID, resultValue);
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
                        FUNC::operation(leftNodeID, rightNodeID, resultValue);
                        selectedPositions[numSelectedValues] = pos;
                        numSelectedValues += resultValue;
                    }
                }
            }
            return numSelectedValues;
        }
    }
};

} // namespace common
} // namespace graphflow
