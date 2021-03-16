#pragma once

#include <functional>
#include <iostream>

#include "src/common/include/vector/node_vector.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct UnaryOperationExecutor {
    // O (operand type), R (result type)
    template<class A, class R, class FUNC = std::function<R(A)>>
    static void execute(ValueVector& operand, ValueVector& result) {
        auto values = (A*)operand.getValues();
        auto resultValues = (R*)result.getValues();
        if (operand.isFlat()) {
            resultValues[0] = FUNC::operation(values[operand.getCurrPos()]);
        } else {
            auto size = operand.size();
            for (uint64_t i = 0; i < size; i++) {
                std::cout << "values[" << i << "] = " << values[i] << std::endl;
                resultValues[i] = FUNC::operation(values[i]);
                std::cout << "resultValues[" << i << "] = " << resultValues[i] << std::endl;
            }
        }
    }

    // A (operand type), R (result type)
    template<class A, class R, class FUNC = std::function<R(A)>>
    static void executeOnNodeIDVector(NodeIDVector& operand, ValueVector& result) {
        auto values = (A*)operand.getValues();
        auto resultValues = (R*)result.getValues();
        auto isSequence = operand.getIsSequence();
        if (operand.isFlat()) {
            nodeID_t nodeID;
            nodeID.offset =
                isSequence ? (values[0] + operand.getCurrPos()) : values[operand.getCurrPos()];
            nodeID.label = operand.getCommonLabel();
            resultValues[0] = FUNC::operation(nodeID);
        } else {
            auto numBytesForNode = operand.getCompressionScheme().getNumTotalBytes();
            auto numBytesForOffset = operand.getCompressionScheme().getNumBytesForOffset();
            auto isLabelCommon = operand.getCompressionScheme().getNumBytesForLabel() == 0;
            auto numBytesForLabel = isLabelCommon ?
                                        sizeof(label_t) :
                                        operand.getCompressionScheme().getNumBytesForLabel();
            auto commonLabel = operand.getCommonLabel();
            auto size = operand.size();
            for (uint64_t i = 0; i < size; i++) {
                nodeID_t nodeID;
                nodeID.offset = isSequence ? (values[0] + i) : values[i];
                memcpy(&nodeID.label,
                    isLabelCommon ? ((uint8_t*)&commonLabel) :
                                    ((uint8_t*)values + (i * numBytesForNode) + numBytesForOffset),
                    numBytesForLabel);
                resultValues[i] = FUNC::operation(nodeID);
            }
        }
    }
};

} // namespace common
} // namespace graphflow
