#pragma once

#include "function/unary_operation_executor.h"

namespace kuzu {
namespace function {

struct NullOperationExecutor {

    template<typename FUNC>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        assert(result.dataType.getLogicalTypeID() == common::LogicalTypeID::BOOL);
        auto resultValues = (uint8_t*)result.getData();
        if (operand.state->isFlat()) {
            auto pos = operand.state->selVector->selectedPositions[0];
            FUNC::operation(
                operand.getValue<uint8_t>(pos), (bool)operand.isNull(pos), resultValues[pos]);
        } else {
            if (operand.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    FUNC::operation(
                        operand.getValue<uint8_t>(i), (bool)operand.isNull(i), resultValues[i]);
                }
            } else {
                for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                    auto pos = operand.state->selVector->selectedPositions[i];
                    FUNC::operation(operand.getValue<uint8_t>(pos), (bool)operand.isNull(pos),
                        resultValues[pos]);
                }
            }
        }
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
            auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
            for (auto i = 0ul; i < operand.state->selVector->selectedSize; i++) {
                auto pos = operand.state->selVector->selectedPositions[i];
                selectOnValue<FUNC>(operand, pos, numSelectedValues, selectedPositionsBuffer);
            }
            selVector.selectedSize = numSelectedValues;
            return numSelectedValues > 0;
        }
    }

    template<typename FUNC>
    static void selectOnValue(common::ValueVector& operand, uint64_t operandPos,
        uint64_t& numSelectedValues, common::sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        FUNC::operation(
            operand.getValue<uint8_t>(operandPos), operand.isNull(operandPos), resultValue);
        selectedPositionsBuffer[numSelectedValues] = operandPos;
        numSelectedValues += resultValue == true;
    }
};

} // namespace function
} // namespace kuzu
