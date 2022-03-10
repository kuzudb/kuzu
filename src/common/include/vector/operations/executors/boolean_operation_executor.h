#pragma once

#include "src/common/include/vector/operations/executors/binary_operation_executor.h"

namespace graphflow {
namespace common {

/**
 * Binary boolean operation requires special executor implementation because it's truth table
 * handles null differently (e.g. NULL OR TRUE = TRUE). Note that unary boolean operation (currently
 * only NOT) does not require special implementation because NOT NULL = NULL.
 */
struct BinaryBooleanOperationExecutor {

    template<typename FUNC>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        assert(left.dataType == BOOL);
        assert(right.dataType == BOOL);
        assert(result.dataType == BOOL);
        if (left.state->isFlat() && right.state->isFlat()) {
            executeBothFlat<FUNC>(left, right, result);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            executeFlatUnFlat<FUNC>(left, right, result);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            executeUnFlatFlat<FUNC>(left, right, result);
        } else {
            executeBothUnFlat<FUNC>(left, right, result);
        }
    }

    template<typename FUNC>
    static void executeBothFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        executeOnValue<FUNC>(left, right, result, lPos, rPos, resPos);
    }

    template<typename FUNC>
    static void executeFlatUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto lPos = left.state->getPositionOfCurrIdx();
        if (right.state->isUnfiltered()) {
            for (auto i = 0u; i < right.state->selectedSize; ++i) {
                executeOnValue<FUNC>(left, right, result, lPos, i, i);
            }
        } else {
            for (auto i = 0u; i < right.state->selectedSize; ++i) {
                auto rPos = right.state->selectedPositions[i];
                executeOnValue<FUNC>(left, right, result, lPos, rPos, rPos);
            }
        }
    }

    template<typename FUNC>
    static void executeUnFlatFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        auto rPos = right.state->getPositionOfCurrIdx();
        if (left.state->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                executeOnValue<FUNC>(left, right, result, i, rPos, i);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto lPos = left.state->selectedPositions[i];
                executeOnValue<FUNC>(left, right, result, lPos, rPos, lPos);
            }
        }
    }

    template<typename FUNC>
    static void executeBothUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        if (left.state->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                executeOnValue<FUNC>(left, right, result, i, i, i);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto pos = left.state->selectedPositions[i];
                executeOnValue<FUNC>(left, right, result, pos, pos, pos);
            }
        }
    }

    template<typename FUNC>
    static inline void executeOnValue(ValueVector& left, ValueVector& right, ValueVector& result,
        uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        BinaryOperationExecutor::executeOnValue<bool, bool, uint8_t, FUNC>(
            left, right, result, lPos, rPos, resPos);
        result.setNull(resPos, result.values[resPos] == operation::NULL_BOOL);
    }

    template<typename FUNC>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        assert(left.dataType == BOOL);
        assert(right.dataType == BOOL);
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<FUNC>(left, right);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<FUNC>(left, right, selectedPositions);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<FUNC>(left, right, selectedPositions);
        } else {
            return selectBothUnFlat<FUNC>(left, right, selectedPositions);
        }
    }

    template<typename FUNC>
    static uint64_t selectBothFlat(ValueVector& left, ValueVector& right) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto lValues = (bool*)left.values;
        auto rValues = (bool*)right.values;
        uint8_t resultValue = 0;
        FUNC::operation(lValues[lPos], rValues[rPos], resultValue, (bool)left.isNull(lPos),
            (bool)right.isNull(rPos));
        return resultValue == true;
    }

    template<typename FUNC>
    static uint64_t selectFlatUnFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto lPos = left.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        if (right.state->isUnfiltered()) {
            for (auto i = 0u; i < right.state->selectedSize; ++i) {
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, lPos, i, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < right.state->selectedSize; ++i) {
                auto rPos = right.state->selectedPositions[i];
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, lPos, rPos, rPos, numSelectedValues, selectedPositions);
            }
        }
        return numSelectedValues;
    }

    template<typename FUNC>
    static uint64_t selectUnFlatFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        auto rPos = right.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        if (left.state->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, i, rPos, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto lPos = left.state->selectedPositions[i];
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, lPos, rPos, lPos, numSelectedValues, selectedPositions);
            }
        }
        return numSelectedValues;
    }

    template<typename FUNC>
    static uint64_t selectBothUnFlat(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        uint64_t numSelectedValues = 0;
        if (left.state->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, i, i, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto pos = left.state->selectedPositions[i];
                BinaryOperationExecutor::selectOnValue<bool, bool, FUNC>(
                    left, right, pos, pos, pos, numSelectedValues, selectedPositions);
            }
        }
        return numSelectedValues;
    }
};

} // namespace common
} // namespace graphflow
