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
    static inline void executeBothFlat(common::SelectedVector left, common::SelectedVector right,
        common::SelectedVector result) {
        auto lPos = left.sel[0];
        auto rPos = right.sel[0];
        auto resPos = result.sel[0];
        executeOnValue<FUNC>(left.vec, right.vec, result.vec, lPos, rPos, resPos);
    }

    template<typename FUNC>
    static void executeFlatUnFlat(common::SelectedVector left, common::SelectedVector right,
        common::SelectedVector result) {
        auto lPos = left.sel[0];
        auto& rightSelVector = right.sel;
        if (rightSelVector.isUnfiltered()) {
            if (right.vec.hasNoNullsGuarantee() && !left.vec.isNull(lPos)) {
                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, lPos, i, i);
                }
            } else {
                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, lPos, i, i);
                }
            }
        } else {
            if (right.vec.hasNoNullsGuarantee() && !left.vec.isNull(lPos)) {
                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                    auto rPos = right.sel[i];
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, lPos, rPos, rPos);
                }
            } else {
                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                    auto rPos = right.sel[i];
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, lPos, rPos, rPos);
                }
            }
        }
    }

    template<typename FUNC>
    static void executeUnFlatFlat(common::SelectedVector left, common::SelectedVector right,
        common::SelectedVector result) {
        auto rPos = right.sel[0];
        auto& leftSelVector = left.sel;
        if (leftSelVector.isUnfiltered()) {
            if (left.vec.hasNoNullsGuarantee() && !right.vec.isNull(rPos)) {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, i, rPos, i);
                }
            } else {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, i, rPos, i);
                }
            }
        } else {
            if (left.vec.hasNoNullsGuarantee() && !right.vec.isNull(rPos)) {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    auto lPos = left.sel[i];
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, lPos, rPos, lPos);
                }
            } else {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    auto lPos = left.sel[i];
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, lPos, rPos, lPos);
                }
            }
        }
    }

    template<typename FUNC>
    static void executeBothUnFlat(common::SelectedVector left, common::SelectedVector right,
        common::SelectedVector result) {
        KU_ASSERT(&left.sel == &right.sel);
        auto& leftSelVector = left.sel;
        if (leftSelVector.isUnfiltered()) {
            if (left.vec.hasNoNullsGuarantee() && right.vec.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, i, i, i);
                }
            } else {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, i, i, i);
                }
            }
        } else {
            if (left.vec.hasNoNullsGuarantee() && right.vec.hasNoNullsGuarantee()) {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    auto pos = left.sel[i];
                    executeOnValueNoNull<FUNC>(left.vec, right.vec, result.vec, pos, pos, pos);
                }
            } else {
                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                    auto pos = left.sel[i];
                    executeOnValue<FUNC>(left.vec, right.vec, result.vec, pos, pos, pos);
                }
            }
        }
    }

    template<typename FUNC>
    static void execute(common::SelectedVector left, common::SelectedVector right,
        common::SelectedVector result) {
        KU_ASSERT(left.vec.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL &&
                  right.vec.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL &&
                  result.vec.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL);
        if (left.vec.state->isFlat() && right.vec.state->isFlat()) {
            executeBothFlat<FUNC>(left, right, result);
        } else if (left.vec.state->isFlat() && !right.vec.state->isFlat()) {
            executeFlatUnFlat<FUNC>(left, right, result);
        } else if (!left.vec.state->isFlat() && right.vec.state->isFlat()) {
            executeUnFlatFlat<FUNC>(left, right, result);
        } else {
            executeBothUnFlat<FUNC>(left, right, result);
        }
    }

    template<class FUNC>
    static void selectOnValue(common::ValueVector& left, common::ValueVector& right, uint64_t lPos,
        uint64_t rPos, uint64_t resPos, uint64_t& numSelectedValues,
        std::span<common::sel_t> selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<uint8_t>(lPos), right.getValue<uint8_t>(rPos), resultValue,
            left.isNull(lPos), right.isNull(rPos));
        selectedPositionsBuffer[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<typename FUNC>
    static bool selectBothFlat(common::ValueVector& left, common::ValueVector& right) {
        auto lPos = left.state->getSelVector()[0];
        auto rPos = right.state->getSelVector()[0];
        uint8_t resultValue = 0;
        FUNC::operation(left.getValue<bool>(lPos), right.getValue<bool>(rPos), resultValue,
            (bool)left.isNull(lPos), (bool)right.isNull(rPos));
        return resultValue == true;
    }

    template<typename FUNC>
    static bool selectFlatUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector) {
        auto lPos = left.state->getSelVector()[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& rightSelVector = right.state->getSelVector();
        if (rightSelVector.isUnfiltered()) {
            for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                selectOnValue<FUNC>(left, right, lPos, i, i, numSelectedValues,
                    selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
                auto rPos = right.state->getSelVector()[i];
                selectOnValue<FUNC>(left, right, lPos, rPos, rPos, numSelectedValues,
                    selectedPositionsBuffer);
            }
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool selectUnFlatFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector) {
        auto rPos = right.state->getSelVector()[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& leftSelVector = left.state->getSelVector();
        if (leftSelVector.isUnfiltered()) {
            for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                selectOnValue<FUNC>(left, right, i, rPos, i, numSelectedValues,
                    selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                auto lPos = left.state->getSelVector()[i];
                selectOnValue<FUNC>(left, right, lPos, rPos, lPos, numSelectedValues,
                    selectedPositionsBuffer);
            }
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool selectBothUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector) {
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& leftSelVector = left.state->getSelVector();
        if (leftSelVector.isUnfiltered()) {
            for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                selectOnValue<FUNC>(left, right, i, i, i, numSelectedValues,
                    selectedPositionsBuffer);
            }
        } else {
            for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
                auto pos = left.state->getSelVector()[i];
                selectOnValue<FUNC>(left, right, pos, pos, pos, numSelectedValues,
                    selectedPositionsBuffer);
            }
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    template<typename FUNC>
    static bool select(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector) {
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
    static inline void executeOnValue(common::ValueVector& operand, uint64_t operandPos,
        common::ValueVector& result, uint64_t resultPos) {
        auto resultValues = (uint8_t*)result.getData();
        FUNC::operation(operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos),
            resultValues[resultPos]);
        result.setNull(resultPos, result.getValue<uint8_t>(resultPos) == NULL_BOOL);
    }

    template<typename FUNC>
    static void executeSwitch(common::SelectedVector operand, common::SelectedVector result) {
        result.vec.resetAuxiliaryBuffer();
        auto& operandSelVector = operand.sel;
        if (operand.vec.state->isFlat()) {
            auto pos = operand.sel[0];
            auto resultPos = result.sel[0];
            executeOnValue<FUNC>(operand.vec, pos, result.vec, resultPos);
        } else {
            if (operandSelVector.isUnfiltered()) {
                for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                    executeOnValue<FUNC>(operand.vec, i, result.vec, i);
                }
            } else {
                for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                    auto pos = operand.sel[i];
                    executeOnValue<FUNC>(operand.vec, pos, result.vec, pos);
                }
            }
        }
    }

    template<typename FUNC>
    static inline void execute(common::SelectedVector operand, common::SelectedVector result) {
        executeSwitch<FUNC>(operand, result);
    }

    template<typename FUNC>
    static inline void selectOnValue(common::ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, std::span<common::sel_t> selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos),
            resultValue);
        selectedPositionsBuffer[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }

    template<typename FUNC>
    static bool select(common::ValueVector& operand, common::SelectionVector& selVector) {
        if (operand.state->isFlat()) {
            auto pos = operand.state->getSelVector()[0];
            uint8_t resultValue = 0;
            FUNC::operation(operand.getValue<uint8_t>(pos), operand.isNull(pos), resultValue);
            return resultValue == true;
        } else {
            auto& operandSelVector = operand.state->getSelVector();
            uint64_t numSelectedValues = 0;
            auto selectedPositionBuffer = selVector.getMutableBuffer();
            if (operandSelVector.isUnfiltered()) {
                for (auto i = 0ul; i < operandSelVector.getSelSize(); i++) {
                    selectOnValue<FUNC>(operand, i, numSelectedValues, selectedPositionBuffer);
                }
            } else {
                for (auto i = 0ul; i < operandSelVector.getSelSize(); i++) {
                    auto pos = operand.state->getSelVector()[i];
                    selectOnValue<FUNC>(operand, pos, numSelectedValues, selectedPositionBuffer);
                }
            }
            selVector.setSelSize(numSelectedValues);
            return numSelectedValues > 0;
        }
    }
};

} // namespace function
} // namespace kuzu
