#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    template<class T, class R, class FUNC = std::function<R(T)>>
    static void executeNonBoolOp(ValueVector& operand, ValueVector& result) {
        auto values = (T*)operand.getValues();
        auto resultValues = (R*)result.getValues();
        auto nullMask = operand.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        if (operand.isFlat()) {
            auto pos = operand.owner->getCurrSelectedValuesPos();
            resultNullMask[0] = nullMask[pos];
            if (!resultNullMask[0]) { // not NULL.
                resultValues[0] = FUNC::operation(values[pos]);
            }
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultNullMask[i] = nullMask[pos];
                if (!resultNullMask[i]) { // not NULL.
                    resultValues[i] = FUNC::operation(values[pos]);
                }
            }
        }
        result.owner->numSelectedValues = size;
    }

    template<class FUNC = std::function<uint8_t(uint8_t)>>
    static void executeBoolOp(ValueVector& operand, ValueVector& result) {
        auto values = operand.getValues();
        auto resultValues = result.getValues();
        auto nullMask = operand.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        if (operand.isFlat()) {
            auto pos = operand.owner->getCurrSelectedValuesPos();
            resultValues[0] = FUNC::operation(values[pos], nullMask[pos]);
            resultNullMask[0] = resultValues[0] == NULL_BOOL;
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[i] = FUNC::operation(values[pos], nullMask[pos]);
                resultNullMask[i] = resultValues[i] == NULL_BOOL;
            }
        }
        result.owner->numSelectedValues = size;
    }

    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto resultValues = (R*)result.getValues();
        auto nullMask = operand.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        nodeID_t nodeID{};
        if (operand.isFlat()) {
            auto pos = operand.owner->getCurrSelectedValuesPos();
            operand.readNodeOffsetAndLabel(pos, nodeID);
            resultValues[0] = FUNC::operation(nodeID);
            resultNullMask[0] = nullMask[0];
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                operand.readNodeOffsetAndLabel(pos, nodeID);
                resultValues[i] = FUNC::operation(nodeID);
                resultNullMask[i] = nullMask[i];
            }
        }
        result.owner->numSelectedValues = size;
    }

    template<bool value>
    static void nullMaskCmp(ValueVector& operand, ValueVector& result) {
        auto resultValues = result.getValues();
        auto nullMask = operand.getNullMask();
        uint64_t size;
        if (operand.isFlat()) {
            auto pos = operand.owner->getCurrSelectedValuesPos();
            resultValues[0] = nullMask[pos] == value;
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[i] = nullMask[pos] == value;
            }
        }
        result.owner->numSelectedValues = size;
    }
};

} // namespace common
} // namespace graphflow
