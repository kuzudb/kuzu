#include "src/common/include/vector/operations/vector_cast_operations.h"

#include <cassert>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {

void VectorCastOperations::castStructuredToUnstructuredValue(
    ValueVector& operand, ValueVector& result) {
    assert(operand.dataType != UNSTRUCTURED && result.dataType == UNSTRUCTURED);
    auto outValues = (Value*)result.values;
    if (operand.state->isFlat()) {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        switch (operand.dataType) {
        case BOOL: {
            outValues[resPos].primitive.booleanVal = operand.values[pos];
        } break;
        case INT32: {
            outValues[resPos].primitive.int32Val = ((int32_t*)operand.values)[pos];
        } break;
        case DOUBLE: {
            outValues[resPos].primitive.doubleVal = ((double_t*)operand.values)[pos];
        } break;
        default:
            assert(false);
        }
    } else {
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
            assert(false);
        }
    }
}

void VectorCastOperations::castStructuredToStringValue(ValueVector& operand, ValueVector& result) {
    assert((operand.dataType == INT32 || operand.dataType == DOUBLE || operand.dataType == BOOL) &&
           result.dataType == STRING);
    auto outValues = (gf_string_t*)result.values;
    if (operand.state->isFlat()) {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        switch (operand.dataType) {
        case BOOL: {
            outValues[resPos].set(operand.values[pos] == TRUE ?
                                      "True" :
                                      (operand.values[pos] == FALSE ? "False" : ""));

        } break;
        case INT32: {
            outValues[resPos].set(to_string(((int32_t*)operand.values)[pos]));
        } break;
        case DOUBLE: {
            outValues[resPos].set(to_string(((double_t*)operand.values)[pos]));
        } break;
        default:
            assert(false);
        }
    } else {
        switch (operand.dataType) {
        case BOOL: {
            for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].set(operand.values[pos] == TRUE ?
                                       "True" :
                                       (operand.values[pos] == FALSE ? "False" : ""));
            }
        } break;
        case INT32: {
            auto intValues = (int32_t*)operand.values;
            for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].set(to_string(intValues[pos]));
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].set(to_string(doubleValues[pos]));
            }
        } break;
        default:
            assert(false);
        }
    }
}

void VectorCastOperations::castUnstructuredToBoolValue(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType == UNSTRUCTURED && result.dataType == BOOL);
    auto inValues = (Value*)result.values;
    if (!operand.state->isFlat()) {
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            if (inValues[pos].dataType != BOOL) {
                throw std::invalid_argument("Don’t know how to treat that as a predicate: “" +
                                            DataTypeNames[inValues[pos].dataType] + "(" +
                                            inValues[pos].toString() + ")”.");
            }
            result.values[pos] = inValues[pos].primitive.booleanVal;
        }
    } else {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        if (inValues[pos].dataType != BOOL) {
            throw std::invalid_argument("Don’t know how to treat that as a predicate: “" +
                                        DataTypeNames[inValues[pos].dataType] + "(" +
                                        inValues[pos].toString() + ")”.");
        }
        result.values[resPos] = inValues[pos].primitive.booleanVal;
    }
}

} // namespace common
} // namespace graphflow
