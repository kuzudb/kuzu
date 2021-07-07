#include "src/common/include/vector/operations/vector_cast_operations.h"

#include <cassert>

#include "src/common/include/date.h"
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
        case INT64: {
            outValues[resPos].val.int64Val = ((int64_t*)operand.values)[pos];
        } break;
        case DOUBLE: {
            outValues[resPos].val.doubleVal = ((double_t*)operand.values)[pos];
        } break;
        case DATE: {
            outValues[resPos].val.dateVal = ((date_t*)operand.values)[pos];
        } break;
        case STRING: {
            auto& operandVal = ((gf_string_t*)operand.values)[pos];
            result.allocateStringOverflowSpace(outValues[resPos].val.strVal, operandVal.len);
            outValues[resPos].val.strVal.set(operandVal);
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
        case INT64: {
            auto intValues = (int64_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.int64Val = intValues[pos];
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.doubleVal = doubleValues[pos];
            }
        } break;
        case DATE: {
            auto dateValues = (date_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                outValues[pos].val.dateVal = dateValues[pos];
            }
        } break;
        case STRING: {
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                auto& operandVal = ((gf_string_t*)operand.values)[pos];
                result.allocateStringOverflowSpace(outValues[pos].val.strVal, operandVal.len);
                outValues[pos].val.strVal.set(operandVal);
            }
        } break;
        default:
            assert(false);
        }
    }
}

void VectorCastOperations::castStructuredToStringValue(ValueVector& operand, ValueVector& result) {
    assert((operand.dataType == INT64 || operand.dataType == DOUBLE || operand.dataType == BOOL ||
               operand.dataType == DATE) &&
           result.dataType == STRING);
    if (operand.state->isFlat()) {
        auto pos = operand.state->getCurrSelectedValuesPos();
        auto resPos = result.state->getCurrSelectedValuesPos();
        string val;
        switch (operand.dataType) {
        case BOOL: {
            val = TypeUtils::toString(operand.values[pos]);
        } break;
        case INT64: {
            val = TypeUtils::toString(((int64_t*)operand.values)[pos]);
        } break;
        case DOUBLE: {
            val = TypeUtils::toString(((double_t*)operand.values)[pos]);
        } break;
        case DATE: {
            val = Date::toString(((date_t*)operand.values)[pos]);
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
                result.addString(pos, TypeUtils::toString(operand.values[pos]));
            }
        } break;
        case INT64: {
            auto intValues = (int64_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, TypeUtils::toString(intValues[pos]));
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, TypeUtils::toString(doubleValues[pos]));
            }
        } break;
        case DATE: {
            for (auto i = 0u; i < operand.state->size; i++) {
                auto pos = operand.state->selectedValuesPos[i];
                result.addString(pos, Date::toString(((date_t*)operand.values)[pos]));
            }
        } break;
        default:
            assert(false);
        }
    }
}

void VectorCastOperations::castUnstructuredToBoolValue(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType == UNSTRUCTURED && result.dataType == BOOL);
    auto inValues = (Value*)operand.values;
    if (!operand.state->isFlat()) {
        for (auto i = 0u; i < operand.state->size; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            if (inValues[pos].dataType != BOOL) {
                throw std::invalid_argument("Don’t know how to treat that as a predicate: “" +
                                            DataTypeNames[inValues[pos].dataType] + "(" +
                                            inValues[pos].toString() + ")”.");
            }
            result.values[pos] = inValues[pos].val.booleanVal;
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
