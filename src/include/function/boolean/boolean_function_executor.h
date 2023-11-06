#pragma once

#include "boolean_functions.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

/**
 * Binary boolean function requires special executor implementation because it's truth table
 * handles null differently (e.g. NULL OR TRUE = TRUE). Note that unary boolean operation (currently
 * only NOT) does not require special implementation because NOT NULL = NULL.
 */
struct BinaryBooleanFunctionExecutor {

    template<typename FUNC>
    static inline void executeOnValueNoNull(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto resValues = (uint8_t*)result.getData();
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos),
            resValues[resPos], false /* isLeftNull */, false /* isRightNull */);
        result.setNull(resPos, false /* isNull */);
    }

    template<typename FUNC>
    static inline void executeOnValue(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, uint64_t lPos, uint64_t rPos, uint64_t resPos) {
        auto resValues = (uint8_t*)result.getData();
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos),
            resValues[resPos], left.isNull(lPos), right.isNull(rPos));
        result.setNull(resPos, result.getValue<uint8_t>(resPos) == NULL_BOOL);
    }

    template<typename FUNC>
    static inline void executeBothFlat(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        auto lPos = left.state->selVector->selectedPositions[0];
        auto rPos = right.state->selVector->selectedPositions[0];
        auto resPos = result.state->selVector->selectedPositions[0];
        executeOnValue<FUNC>(left, right, result, lPos, rPos, resPos);
    }

    template<typename FUNC>
    static void executeFlatUnFlat(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        auto lPos = left.state->selVector->selectedPositions[0];
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
    static void executeUnFlatFlat(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        auto rPos = right.state->selVector->selectedPositions[0];
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
    static void executeBothUnFlat(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        KU_ASSERT(left.state == right.state);
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
    static void execute(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        KU_ASSERT(left.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL &&
                  right.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL &&
                  result.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL);
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
    static void selectOnValue(common::ValueVector& left, common::ValueVector& right, uint64_t lPos,
        uint64_t rPos, uint64_t resPos, uint64_t& numSelectedValues,
        common::sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos), resultValue,
            left.isNull(lPos), right.isNull(rPos));
        selectedPositionsBuffer[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<typename FUNC>
    static bool selectBothFlat(common::ValueVector& left, common::ValueVector& right) {
        auto lPos = left.state->selVector->selectedPositions[0];
        auto rPos = right.state->selVector->selectedPositions[0];
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<bool>(lPos), right.getValue<bool>(rPos), resultValue,
            (bool)left.isNull(lPos), (bool)right.isNull(rPos));
        return resultValue == true;
    }

    template<typename FUNC>
    static bool selectFlatUnFlat(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        auto lPos = left.state->selVector->selectedPositions[0];
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
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        auto rPos = right.state->selVector->selectedPositions[0];
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
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
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
    static bool select(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        KU_ASSERT(left.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL &&
                  right.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL);
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
        common::ValueVector& operand, uint64_t operandPos, common::ValueVector& result) {
        auto resultValues = (uint8_t*)result.getData();
        FUNC::operation(operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos),
            resultValues[operandPos]);
        result.setNull(operandPos, result.getValue<uint8_t>(operandPos) == NULL_BOOL);
    }

    template<typename FUNC>
    static void executeSwitch(common::ValueVector& operand, common::ValueVector& result) {
        result.resetAuxiliaryBuffer();
        if (operand.state->isFlat()) {
            auto pos = operand.state->selVector->selectedPositions[0];
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
    static inline void execute(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<FUNC>(operand, result);
    }

    template<typename FUNC>
    static inline void selectOnValue(common::ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, common::sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(
            operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos), resultValue);
        selectedPositionsBuffer[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }

    template<typename FUNC>
    static bool select(common::ValueVector& operand, common::SelectionVector& selVector) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->selVector->selectedPositions[0];
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
