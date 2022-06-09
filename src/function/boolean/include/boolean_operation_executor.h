#pragma once

#include "src/function/boolean/operations/include/boolean_operations.h"
#include "src/function/include/binary_operation_executor.h"

namespace graphflow {
namespace function {

/**
 * Binary boolean operation requires special executor implementation because it's truth table
 * handles null differently (e.g. NULL OR TRUE = TRUE). Note that unary boolean operation (currently
 * only NOT) does not require special implementation because NOT NULL = NULL.
 */
struct BinaryBooleanOperationExecutor {

    template<typename FUNC>
    static inline void executeOnValue(ValueVector& left, ValueVector& right, ValueVector& result,
        uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto lValues = (uint8_t*)left.values;
        auto rValues = (uint8_t*)right.values;
        auto resValues = (uint8_t*)result.values;
        FUNC::operation(
            lValues[lPos], rValues[rPos], resValues[resPos], left.isNull(lPos), right.isNull(rPos));
        result.setNull(resPos, result.values[resPos] == operation::NULL_BOOL);
    }

    template<typename FUNC>
    static void executeBothFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = left.state;
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        executeOnValue<FUNC>(left, right, result, lPos, rPos, resPos);
    }

    template<typename FUNC>
    static void executeFlatUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = right.state;
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
        result.state = left.state;
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
        assert(left.state == right.state);
        result.state = left.state;
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
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        assert(left.dataType.typeID == BOOL && right.dataType.typeID == BOOL &&
               result.dataType.typeID == BOOL);
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

    template<class FUNC>
    static void selectOnValue(ValueVector& left, ValueVector& right, uint64_t lPos, uint64_t rPos,
        uint64_t resPos, uint64_t& numSelectedValues, sel_t* selectedPositions) {
        auto lValues = (uint8_t*)left.values;
        auto rValues = (uint8_t*)right.values;
        uint8_t resultValue = 0;
        FUNC::operation(
            lValues[lPos], rValues[rPos], resultValue, left.isNull(lPos), right.isNull(rPos));
        selectedPositions[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
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
                selectOnValue<FUNC>(left, right, lPos, i, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < right.state->selectedSize; ++i) {
                auto rPos = right.state->selectedPositions[i];
                selectOnValue<FUNC>(
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
                selectOnValue<FUNC>(left, right, i, rPos, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto lPos = left.state->selectedPositions[i];
                selectOnValue<FUNC>(
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
                selectOnValue<FUNC>(left, right, i, i, i, numSelectedValues, selectedPositions);
            }
        } else {
            for (auto i = 0u; i < left.state->selectedSize; ++i) {
                auto pos = left.state->selectedPositions[i];
                selectOnValue<FUNC>(
                    left, right, pos, pos, pos, numSelectedValues, selectedPositions);
            }
        }
        return numSelectedValues;
    }

    template<typename FUNC>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        assert(left.dataType.typeID == BOOL && right.dataType.typeID == BOOL);
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
};

struct UnaryBooleanOperationExecutor {

    template<typename FUNC>
    static void executeOnValue(ValueVector& operand, uint64_t operandPos, ValueVector& result) {
        auto operandValues = (uint8_t*)operand.values;
        auto resultValues = (uint8_t*)result.values;
        FUNC::operation(
            operandValues[operandPos], operand.isNull(operandPos), resultValues[operandPos]);
        result.setNull(operandPos, result.values[operandPos] == operation::NULL_BOOL);
    }

    template<typename FUNC>
    static void executeSwitch(ValueVector& operand, ValueVector& result) {
        result.resetOverflowBuffer();
        result.state = operand.state;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            executeOnValue<FUNC>(operand, pos, result);
        } else {
            if (operand.state->isUnfiltered()) {
                for (auto i = 0u; i < operand.state->selectedSize; i++) {
                    executeOnValue<FUNC>(operand, i, result);
                }
            } else {
                for (auto i = 0u; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    executeOnValue<FUNC>(operand, pos, result);
                }
            }
        }
    }

    template<typename FUNC>
    static void execute(ValueVector& operand, ValueVector& result) {
        executeSwitch<FUNC>(operand, result);
    }

    template<typename FUNC>
    static void selectOnValue(ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, sel_t* selectedPositions) {
        uint8_t resultValue = 0;
        auto operandValues = (uint8_t*)operand.values;
        FUNC::operation(operandValues[operandPos], operand.isNull(operandPos), resultValue);
        selectedPositions[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }

    template<typename FUNC>
    static uint64_t select(ValueVector& operand, sel_t* selectedPositions) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue = 0;
            FUNC::operation(operand.values[pos], operand.isNull(pos), resultValue);
            return resultValue == true;
        } else {
            uint64_t numSelectedValues = 0;
            if (operand.state->isUnfiltered()) {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    selectOnValue<FUNC>(operand, i, numSelectedValues, selectedPositions);
                }
            } else {
                for (auto i = 0ul; i < operand.state->selectedSize; i++) {
                    auto pos = operand.state->selectedPositions[i];
                    selectOnValue<FUNC>(operand, pos, numSelectedValues, selectedPositions);
                }
            }
            return numSelectedValues;
        }
    }
};

} // namespace function
} // namespace graphflow
