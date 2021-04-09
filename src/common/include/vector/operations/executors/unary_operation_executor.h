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
            auto pos = operand.getCurrSelectedValuesPos();
            resultNullMask[pos] = nullMask[pos];
            if (!resultNullMask[pos]) { // not NULL.
                resultValues[pos] = FUNC::operation(values[pos]);
            }
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultNullMask[pos] = nullMask[pos];
                if (!resultNullMask[pos]) { // not NULL.
                    resultValues[pos] = FUNC::operation(values[pos]);
                }
            }
        }
        result.setNumSelectedValues(size);
    }

    template<class FUNC = std::function<uint8_t(uint8_t)>>
    static void executeBoolOp(ValueVector& operand, ValueVector& result) {
        auto values = operand.getValues();
        auto resultValues = result.getValues();
        auto nullMask = operand.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        if (operand.isFlat()) {
            auto pos = operand.getCurrSelectedValuesPos();
            resultValues[pos] = FUNC::operation(values[pos], nullMask[pos]);
            resultNullMask[pos] = resultValues[pos] == NULL_BOOL;
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[pos] = FUNC::operation(values[pos], nullMask[pos]);
                resultNullMask[pos] = resultValues[pos] == NULL_BOOL;
            }
        }
        result.setNumSelectedValues(size);
    }

    template<class R, class FUNC = std::function<R(nodeID_t)>>
    static void executeOnNodeIDVector(ValueVector& operand, ValueVector& result) {
        auto resultValues = (R*)result.getValues();
        auto nullMask = operand.getNullMask();
        auto resultNullMask = result.getNullMask();
        uint64_t size;
        nodeID_t nodeID{};
        if (operand.isFlat()) {
            auto pos = operand.getCurrSelectedValuesPos();
            operand.readNodeOffsetAndLabel(pos, nodeID);
            resultValues[pos] = FUNC::operation(nodeID);
            resultNullMask[pos] = nullMask[pos];
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                operand.readNodeOffsetAndLabel(pos, nodeID);
                resultValues[pos] = FUNC::operation(nodeID);
                resultNullMask[pos] = nullMask[pos];
            }
        }
        result.setNumSelectedValues(size);
    }

    template<bool value>
    static void nullMaskCmp(ValueVector& operand, ValueVector& result) {
        auto resultValues = result.getValues();
        auto nullMask = operand.getNullMask();
        uint64_t size;
        if (operand.isFlat()) {
            auto pos = operand.getCurrSelectedValuesPos();
            resultValues[pos] = nullMask[pos] == value;
            size = 1;
        } else {
            auto selectedValuesPos = operand.getSelectedValuesPos();
            size = operand.getNumSelectedValues();
            for (uint64_t i = 0; i < size; i++) {
                auto pos = selectedValuesPos[i];
                resultValues[pos] = nullMask[pos] == value;
            }
        }
        result.setNumSelectedValues(size);
    }
};

} // namespace common
} // namespace graphflow
