#include "src/common/include/vector/operations/vector_cast_operations.h"

#include <cassert>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {

void VectorCastOperations::castStructuredToUnstructuredValue(
    ValueVector& operand, ValueVector& result) {
    assert(operand.dataType != UNSTRUCTURED && result.dataType == UNSTRUCTURED);
    auto outValues = (Value*)result.values;
    switch (operand.dataType) {
    case BOOL: {
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.booleanVal = operand.values[pos];
        }
    } break;
    case INT32: {
        auto intValues = (int32_t*)operand.values;
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.int32Val = intValues[pos];
        }
    } break;
    case DOUBLE: {
        auto doubleValues = (double_t*)operand.values;
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.doubleVal = doubleValues[pos];
        }
    } break;
    default:
        assert(false); // should never happen.
    }
}

void VectorCastOperations::castUnstructuredToBoolValue(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType == UNSTRUCTURED && result.dataType == BOOL);
    auto inValues = (Value*)result.values;
    for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
        auto pos = operand.state->selectedValuesPos[i];
        if (inValues[pos].dataType != BOOL) {
            throw std::invalid_argument("don’t know how to treat that as a predicate: “" +
                                        DataTypeNames[inValues[pos].dataType] + "(" +
                                        inValues[pos].toString() + ")”.");
        }
        result.values[pos] = inValues[pos].primitive.booleanVal;
    }
}

} // namespace common
} // namespace graphflow
