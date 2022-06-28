#pragma once

#include <functional>

#include "src/common/include/type_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace function {

/**
 * Binary operator assumes operation with null returns null. This does NOT applies to binary boolean
 * operations (e.g. AND, OR, XOR).
 */

struct BinaryOperationWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        void* leftValueVector, void* rightValueVector, void* resultValueVector) {
        OP::operation(left, right, result);
    }
};

struct BinaryStringAndListOperationWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        void* leftValueVector, void* rightValueVector, void* resultValueVector) {
        OP::operation(left, right, result, *(ValueVector*)resultValueVector);
    }
};

struct BinaryListPosAndContainsOperationWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        void* leftValueVector, void* rightValueVector, void* resultValueVector) {
        OP::operation(left, right, result, ((ValueVector*)leftValueVector)->dataType,
            ((ValueVector*)rightValueVector)->dataType);
    }
};

struct BinaryOperationExecutor {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static inline void executeOnValue(ValueVector& left, ValueVector& right,
        ValueVector& resultValueVector, uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        OP_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            ((LEFT_TYPE*)left.values)[lPos], ((RIGHT_TYPE*)right.values)[rPos],
            ((RESULT_TYPE*)resultValueVector.values)[resPos], (void*)&left, (void*)&right,
            (void*)&resultValueVector);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeBothFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = left.state;
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
        if (!result.isNull(resPos)) {
            executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, lPos, rPos, resPos);
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = right.state;
        auto lPos = left.state->getPositionOfCurrIdx();
        if (left.isNull(lPos)) {
            result.setAllNull();
        } else if (right.hasNoNullsGuarantee()) {
            if (right.state->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, i, i);
                }
            } else {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    auto rPos = right.state->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, rPos, rPos);
                }
            }
        } else {
            if (right.state->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    result.setNull(i, right.isNull(i)); // left is always not null
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, i, i);
                    }
                }
            } else {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    auto rPos = right.state->selectedPositions[i];
                    result.setNull(rPos, right.isNull(rPos)); // left is always not null
                    if (!result.isNull(rPos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, rPos, rPos);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnFlatFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = left.state;
        auto rPos = right.state->getPositionOfCurrIdx();
        if (right.isNull(rPos)) {
            result.setAllNull();
        } else if (left.hasNoNullsGuarantee()) {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, i, rPos, i);
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    auto lPos = left.state->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, rPos, lPos);
                }
            }
        } else {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    result.setNull(i, left.isNull(i)); // right is always not null
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, i, rPos, i);
                    }
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    auto lPos = left.state->selectedPositions[i];
                    result.setNull(lPos, left.isNull(lPos)); // right is always not null
                    if (!result.isNull(lPos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, rPos, lPos);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeBothUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        assert(left.state == right.state);
        result.state = left.state;
        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
            if (result.state->isUnfiltered()) {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, i, i, i);
                }
            } else {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    auto pos = result.state->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, pos, pos, pos);
                }
            }
        } else {
            if (result.state->isUnfiltered()) {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    result.setNull(i, left.isNull(i) || right.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, i, i, i);
                    }
                }
            } else {
                for (uint64_t i = 0; i < result.state->selectedSize; i++) {
                    auto pos = result.state->selectedPositions[i];
                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, pos, pos, pos);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.resetOverflowBuffer();
        if (left.state->isFlat() && right.state->isFlat()) {
            executeBothFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            executeFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            executeUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result);
        } else if (!left.state->isFlat() && !right.state->isFlat()) {
            executeBothUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result);
        } else {
            assert(false);
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryOperationWrapper>(
            left, right, result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeStringAndList(ValueVector& left, ValueVector& right, ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryStringAndListOperationWrapper>(left, right, result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeListPosAndContains(
        ValueVector& left, ValueVector& right, ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryListPosAndContainsOperationWrapper>(left, right, result);
    }

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static void selectOnValue(ValueVector& left, ValueVector& right, uint64_t lPos, uint64_t rPos,
        uint64_t resPos, uint64_t& numSelectedValues, sel_t* selectedPositions) {
        auto lValues = (LEFT_TYPE*)left.values;
        auto rValues = (RIGHT_TYPE*)right.values;
        uint8_t resultValue = 0;
        FUNC::operation(lValues[lPos], rValues[rPos], resultValue);
        selectedPositions[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static uint64_t selectBothFlat(ValueVector& left, ValueVector& right) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto lValues = (LEFT_TYPE*)left.values;
        auto rValues = (RIGHT_TYPE*)right.values;
        uint8_t resultValue = 0;
        if (!left.isNull(lPos) && !right.isNull(rPos)) {
            FUNC::operation(lValues[lPos], rValues[rPos], resultValue);
        }
        return resultValue == true;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static uint64_t selectFlatUnFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto lPos = left.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        if (left.isNull(lPos)) {
            return numSelectedValues;
        } else if (right.hasNoNullsGuarantee()) {
            if (right.state->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, lPos, i, i, numSelectedValues, selectedPositions);
                }
            } else {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    auto rPos = right.state->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, lPos, rPos, rPos, numSelectedValues, selectedPositions);
                }
            }
        } else {
            if (right.state->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    if (!right.isNull(i)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, lPos, i, i, numSelectedValues, selectedPositions);
                    }
                }
            } else {
                for (auto i = 0u; i < right.state->selectedSize; ++i) {
                    auto rPos = right.state->selectedPositions[i];
                    if (!right.isNull(rPos)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, lPos, rPos, rPos, numSelectedValues, selectedPositions);
                    }
                }
            }
        }
        return numSelectedValues;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static uint64_t selectUnFlatFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto rPos = right.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        if (right.isNull(rPos)) {
            return numSelectedValues;
        } else if (left.hasNoNullsGuarantee()) {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, i, rPos, i, numSelectedValues, selectedPositions);
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    auto lPos = left.state->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, lPos, rPos, lPos, numSelectedValues, selectedPositions);
                }
            }
        } else {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    if (!left.isNull(i)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, i, rPos, i, numSelectedValues, selectedPositions);
                    }
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; ++i) {
                    auto lPos = left.state->selectedPositions[i];
                    if (!left.isNull(lPos)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, lPos, rPos, lPos, numSelectedValues, selectedPositions);
                    }
                }
            }
        }
        return numSelectedValues;
    }

    // Right, left, and result vectors share the same selectedPositions.
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static uint64_t selectBothUnFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        uint64_t numSelectedValues = 0;
        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
            if (left.state->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, i, i, i, numSelectedValues, selectedPositions);
                }
            } else {
                for (auto i = 0u; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                        left, right, pos, pos, pos, numSelectedValues, selectedPositions);
                }
            }
        } else {
            if (left.state->isUnfiltered()) {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto isNull = left.isNull(i) || right.isNull(i);
                    if (!isNull) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, i, i, i, numSelectedValues, selectedPositions);
                    }
                }
            } else {
                for (uint64_t i = 0; i < left.state->selectedSize; i++) {
                    auto pos = left.state->selectedPositions[i];
                    auto isNull = left.isNull(pos) || right.isNull(pos);
                    if (!isNull) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                            left, right, pos, pos, pos, numSelectedValues, selectedPositions);
                    }
                }
            }
        }
        return numSelectedValues;
    }

    // COMPARISON (GT, GTE, LT, LTE, EQ, NEQ), BOOLEAN (AND, OR, XOR)
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selectedPositions);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selectedPositions);
        } else {
            return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selectedPositions);
        }
    }
};

} // namespace function
} // namespace graphflow
