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
            outValues[resPos].val.booleanVal = operand.values[pos];
        } break;
        case INT32: {
            outValues[resPos].val.int32Val = ((int32_t*)operand.values)[pos];
        } break;
        case DOUBLE: {
            outValues[resPos].val.doubleVal = ((double_t*)operand.values)[pos];
        } break;
        default:
            assert(false);
        }
    } else {
        switch (operand.dataType) {
        case BOOL: {
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.booleanVal = operand.values[pos];
            }
        } break;
        case INT32: {
            auto intValues = (int32_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.int32Val = intValues[pos];
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.doubleVal = doubleValues[pos];
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
    if (operand.state->isFlat()) {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        string val;
        switch (operand.dataType) {
        case BOOL: {
            val = operand.values[pos] == TRUE ? "True" :
                                                (operand.values[pos] == FALSE ? "False" : "");
        } break;
        case INT32: {
            val = to_string(((int32_t*)operand.values)[pos]);
        } break;
        case DOUBLE: {
            val = to_string(((double_t*)operand.values)[pos]);
        } break;
        default:
            assert(false);
        }
        result.addString(resPos, val);
    } else {
        switch (operand.dataType) {
        case BOOL: {
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, operand.values[pos] == TRUE ?
                                          "True" :
                                          (operand.values[pos] == FALSE ? "False" : ""));
            }
        } break;
        case INT32: {
            auto intValues = (int32_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, to_string(intValues[pos]));
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, to_string(doubleValues[pos]));
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
        for (auto i = 0u; i < operand.state->size; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            if (inValues[pos].dataType != BOOL) {
                throw std::invalid_argument("Don’t know how to treat that as a predicate: “" +
                                            DataTypeNames[inValues[pos].dataType] + "(" +
                                            inValues[pos].toString() + ")”.");
            }
        }
    } else {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        if (inValues[pos].dataType != BOOL) {
            throw std::invalid_argument("Don’t know how to treat that as a predicate: “" +
                                        DataTypeNames[inValues[pos].dataType] + "(" +
                                        inValues[pos].toString() + ")”.");
        }
        result.values[resPos] = inValues[pos].val.booleanVal;
    }
}

} // namespace common
} // namespace graphflow
