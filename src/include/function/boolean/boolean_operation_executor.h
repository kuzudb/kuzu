#pragma once

#include "boolean_operations.h"
#include "function/binary_operation_executor.h"

namespace kuzu {
namespace function {

/**
 * Binary boolean operation requires special executor implementation because it's truth table
 * handles null differently (e.g. NULL OR TRUE = TRUE). Note that unary boolean operation (currently
 * only NOT) does not require special implementation because NOT NULL = NULL.
 */
struct BinaryBooleanOperationExecutor {

    template<typename FUNC>
    static inline void executeOnValueNoNull(ValueVector& left, ValueVector& right,
        ValueVector& result, uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto resValues = (uint8_t*)result.getData();
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos),
            resValues[resPos], false /* isLeftNull */, false /* isRightNull */);
        result.setNull(resPos, false /* isNull */);
    }

    template<typename FUNC>
    static inline void executeOnValue(ValueVector& left, ValueVector& right, ValueVector& result,
        uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto resValues = (uint8_t*)result.getData();
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos),
            resValues[resPos], left.isNull(lPos), right.isNull(rPos));
        result.setNull(resPos, result.getValue<uint8_t>(resPos) == operation::NULL_BOOL);
    }

    template<typename FUNC>
    static inline void executeBothFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
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
        if (right.state->selVector->isUnfiltered()) {
            if (right.hasNoNullsGuarantee() && !left.isNull(lPos)) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    executeOnValueNoNull<FUNC>(left, right, result, lPos, i, i);
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    executeOnValue<FUNC>(left, right, result, lPos, i, i);
                }
            }
        } else {
            if (right.hasNoNullsGuarantee() && !left.isNull(lPos)) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    executeOnValueNoNull<FUNC>(left, right, result, lPos, rPos, rPos);
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    executeOnValue<FUNC>(left, right, result, lPos, rPos, rPos);
                }
            }
        }
    }

    template<typename FUNC>
    static void executeUnFlatFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        result.state = left.state;
        auto rPos = right.state->getPositionOfCurrIdx();
        if (left.state->selVector->isUnfiltered()) {
            if (left.hasNoNullsGuarantee() && !right.isNull(rPos)) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    executeOnValueNoNull<FUNC>(left, right, result, i, rPos, i);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    executeOnValue<FUNC>(left, right, result, i, rPos, i);
                }
            }
        } else {
            if (left.hasNoNullsGuarantee() && !right.isNull(rPos)) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    executeOnValueNoNull<FUNC>(left, right, result, lPos, rPos, lPos);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    executeOnValue<FUNC>(left, right, result, lPos, rPos, lPos);
                }
            }
        }
    }

    template<typename FUNC>
    static void executeBothUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
        assert(left.state == right.state);
        result.state = left.state;
        if (left.state->selVector->isUnfiltered()) {
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    executeOnValueNoNull<FUNC>(left, right, result, i, i, i);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    executeOnValue<FUNC>(left, right, result, i, i, i);
                }
            }
        } else {
            if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto pos = left.state->selVector->selectedPositions[i];
                    executeOnValueNoNull<FUNC>(left, right, result, pos, pos, pos);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto pos = left.state->selVector->selectedPositions[i];
                    executeOnValue<FUNC>(left, right, result, pos, pos, pos);
                }
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
        uint64_t resPos, uint64_t& numSelectedValues, sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos), resultValue,
            left.isNull(lPos), right.isNull(rPos));
        selectedPositionsBuffer[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<typename FUNC>
    static bool selectBothFlat(ValueVector& left, ValueVector& right) {
        auto lPos = left.state->getPositionOfCurrIdx();
        auto rPos = right.state->getPositionOfCurrIdx();
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<bool>(lPos), right.getValue<bool>(rPos), resultValue,
            (bool)left.isNull(lPos), (bool)right.isNull(rPos));
        return resultValue == true;
    }

    template<typename FUNC>
    static bool selectFlatUnFlat(
        ValueVector& left, ValueVector& right, SelectionVector& selVector) {
        auto lPos = left.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (right.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                selectOnValue<FUNC>(
                    left, right, lPos, i, i, numSelectedValues, selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                auto rPos = right.state->selVector->selectedPositions[i];
                selectOnValue<FUNC>(
                    left, right, lPos, rPos, rPos, numSelectedValues, selectedPositionsBuffer);
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool selectUnFlatFlat(
        ValueVector& left, ValueVector& right, SelectionVector& selVector) {
        auto rPos = right.state->getPositionOfCurrIdx();
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (left.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                selectOnValue<FUNC>(
                    left, right, i, rPos, i, numSelectedValues, selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                auto lPos = left.state->selVector->selectedPositions[i];
                selectOnValue<FUNC>(
                    left, right, lPos, rPos, lPos, numSelectedValues, selectedPositionsBuffer);
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool selectBothUnFlat(
        ValueVector& left, ValueVector& right, SelectionVector& selVector) {
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (left.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                selectOnValue<FUNC>(
                    left, right, i, i, i, numSelectedValues, selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                auto pos = left.state->selVector->selectedPositions[i];
                selectOnValue<FUNC>(
                    left, right, pos, pos, pos, numSelectedValues, selectedPositionsBuffer);
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool select(ValueVector& left, ValueVector& right, SelectionVector& selVector) {
        assert(left.dataType.typeID == BOOL && right.dataType.typeID == BOOL);
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<FUNC>(left, right);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<FUNC>(left, right, selVector);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<FUNC>(left, right, selVector);
        } else {
            return selectBothUnFlat<FUNC>(left, right, selVector);
        }
    }
};

struct UnaryBooleanOperationExecutor {

    template<typename FUNC>
    static inline void executeOnValue(
        ValueVector& operand, uint64_t operandPos, ValueVector& result) {
        auto resultValues = (uint8_t*)result.getData();
        FUNC::operation(operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos),
            resultValues[operandPos]);
        result.setNull(operandPos, result.getValue<uint8_t>(operandPos) == operation::NULL_BOOL);
    }

    template<typename FUNC>
    static void executeSwitch(ValueVector& operand, ValueVector& result) {
        result.resetOverflowBuffer();
        result.state = operand.state;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            executeOnValue<FUNC>(operand, pos, result);
        } else {
            if (operand.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    executeOnValue<FUNC>(operand, i, result);
                }
            } else {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    auto pos = operand.state->selVector->selectedPositions[i];
                    executeOnValue<FUNC>(operand, pos, result);
                }
            }
        }
    }

    template<typename FUNC>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        executeSwitch<FUNC>(operand, result);
    }

    template<typename FUNC>
    static inline void selectOnValue(ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(
            operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos), resultValue);
        selectedPositionsBuffer[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }

    template<typename FUNC>
    static bool select(ValueVector& operand, SelectionVector& selVector) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            uint8_t resultValue = 0;
            FUNC::operation(operand.getValue<uint8_t>(pos), operand.isNull(pos), resultValue);
            return resultValue == true;
        } else {
            uint64_t numSelectedValues = 0;
            auto selectedPositionBuffer = selVector.getSelectedPositionsBuffer();
            if (operand.state->selVector->isUnfiltered()) {
                for (auto i = 0ul; i < operand.state->selVector->selectedSize; i++) {
                    selectOnValue<FUNC>(operand, i, numSelectedValues, selectedPositionBuffer);
                }
            } else {
                for (auto i = 0ul; i < operand.state->selVector->selectedSize; i++) {
                    auto pos = operand.state->selVector->selectedPositions[i];
                    selectOnValue<FUNC>(operand, pos, numSelectedValues, selectedPositionBuffer);
                }
            }
            selVector.selectedSize = numSelectedValues;
            return numSelectedValues > 0;
        }
    }
};

} // namespace function
} // namespace kuzu
