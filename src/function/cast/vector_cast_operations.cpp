#include "include/vector_cast_operations.h"

#include "operations/include/cast_operations.h"

#include "src/common/include/vector/operations/executors/unary_operation_executor.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace function {

void VectorCastOperations::castStructuredToUnstructuredValue(
    ValueVector& operand, ValueVector& result) {
    switch (operand.dataType) {
    case BOOL: {
        UnaryOperationExecutor::execute<uint8_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case INT64: {
        UnaryOperationExecutor::execute<int64_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case DOUBLE: {
        UnaryOperationExecutor::execute<double_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case DATE: {
        UnaryOperationExecutor::execute<date_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case TIMESTAMP: {
        UnaryOperationExecutor::execute<timestamp_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case INTERVAL: {
        UnaryOperationExecutor::execute<interval_t, Value, operation::CastToUnstructured>(
            operand, result);
    } break;
    case STRING: {
        auto outValues = (Value*)result.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            assert(pos == result.state->getPositionOfCurrIdx());
            result.addGFStringToUnstructuredVector(pos, ((gf_string_t*)operand.values)[pos]);
            outValues[pos].dataType = STRING;
        } else {
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addGFStringToUnstructuredVector(pos, ((gf_string_t*)operand.values)[pos]);
                outValues[pos].dataType = STRING;
            }
        }
    } break;
    default:
        assert(false);
    }
}

void VectorCastOperations::castStructuredToString(
    const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
    assert(params.size() == 1);
    auto& operand = *params[0];
    assert((operand.dataType == INT64 || operand.dataType == DOUBLE || operand.dataType == BOOL ||
               operand.dataType == DATE || operand.dataType == TIMESTAMP ||
               operand.dataType == INTERVAL) &&
           result.dataType == STRING);
    if (operand.state->isFlat()) {
        auto pos = operand.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        string val;
        switch (operand.dataType) {
        case BOOL: {
            val = TypeUtils::toString(((bool*)operand.values)[pos]);
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
        case TIMESTAMP: {
            val = Timestamp::toString(((timestamp_t*)operand.values)[pos]);
        } break;
        case INTERVAL: {
            val = Interval::toString(((interval_t*)operand.values)[pos]);
        } break;
        default:
            assert(false);
        }
        result.addString(resPos, val);
    } else {
        switch (operand.dataType) {
        case BOOL: {
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, TypeUtils::toString(((bool*)operand.values)[pos]));
            }
        } break;
        case INT64: {
            auto intValues = (int64_t*)operand.values;
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, TypeUtils::toString(intValues[pos]));
            }
        } break;
        case DOUBLE: {
            auto doubleValues = (double_t*)operand.values;
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, TypeUtils::toString(doubleValues[pos]));
            }
        } break;
        case DATE: {
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, Date::toString(((date_t*)operand.values)[pos]));
            }
        } break;
        case TIMESTAMP: {
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, Timestamp::toString(((timestamp_t*)operand.values)[pos]));
            }
        } break;
        case INTERVAL: {
            for (auto i = 0u; i < operand.state->selectedSize; i++) {
                auto pos = operand.state->selectedPositions[i];
                result.addString(pos, Interval::toString(((interval_t*)operand.values)[pos]));
            }
        } break;
        default:
            assert(false);
        }
    }
}

} // namespace function
} // namespace graphflow
