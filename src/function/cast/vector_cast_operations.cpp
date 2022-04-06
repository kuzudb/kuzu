#include "include/vector_cast_operations.h"

#include "operations/include/cast_operations.h"

#include "src/common/include/vector/value_vector_utils.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace function {

pair<scalar_exec_func, DataType> VectorCastOperations::bindExplicitCastStringToDate(
    const expression_vector& children) {
    validateNumParameters(CAST_TO_DATE_FUNCTION_NAME, children.size(), 1);
    validateParameterType(CAST_TO_DATE_FUNCTION_NAME, *children[0], STRING);
    return make_pair(
        UnaryExecFunction<gf_string_t, date_t, operation::CastStringToDate>, DataType(DATE));
}

pair<scalar_exec_func, DataType> VectorCastOperations::bindExplicitCastStringToTimestamp(
    const expression_vector& children) {
    validateNumParameters(CAST_TO_TIMESTAMP_FUNCTION_NAME, children.size(), 1);
    validateParameterType(CAST_TO_TIMESTAMP_FUNCTION_NAME, *children[0], STRING);
    return make_pair(UnaryExecFunction<gf_string_t, timestamp_t, operation::CastStringToTimestamp>,
        DataType(TIMESTAMP));
}

pair<scalar_exec_func, DataType> VectorCastOperations::bindExplicitCastStringToInterval(
    const expression_vector& children) {
    validateNumParameters(CAST_TO_INTERVAL_FUNCTION_NAME, children.size(), 1);
    validateParameterType(CAST_TO_INTERVAL_FUNCTION_NAME, *children[0], STRING);
    return make_pair(UnaryExecFunction<gf_string_t, interval_t, operation::CastStringToInterval>,
        DataType(INTERVAL));
}

pair<scalar_exec_func, DataType> VectorCastOperations::bindExplicitCastToString(
    const expression_vector& children) {
    validateNumParameters(CAST_TO_STRING_FUNCTION_NAME, children.size(), 1);
    auto dataTypeID = children[0]->dataType.typeID;
    switch (children[0]->dataType.typeID) {
    case BOOL: {
        return make_pair(castToString<bool>, DataType(STRING));
    }
    case INT64: {
        return make_pair(castToString<int64_t>, DataType(STRING));
    }
    case DOUBLE: {
        return make_pair(castToString<double_t>, DataType(STRING));
    }
    case DATE: {
        return make_pair(castToString<date_t>, DataType(STRING));
    }
    case TIMESTAMP: {
        return make_pair(castToString<timestamp_t>, DataType(STRING));
    }
    case INTERVAL: {
        return make_pair(castToString<interval_t>, DataType(STRING));
    }
    case UNSTRUCTURED: {
        return make_pair(castToString<Value>, DataType(UNSTRUCTURED));
    }
    default:
        throw NotImplementedException("Explicit cast from " + Types::dataTypeToString(dataTypeID) +
                                      " to STRING is not supported.");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastToBool(const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != BOOL);
    auto child = children[0];
    switch (children[0]->dataType.typeID) {
    case UNSTRUCTURED: {
        return UnaryExecFunction<Value, uint8_t, operation::CastUnstructuredToBool>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect BOOL. Implicit cast is not supported.");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastToString(const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != STRING);
    auto child = children[0];
    switch (child->dataType.typeID) {
    case UNSTRUCTURED: {
        return castToString<Value>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect STRING. Implicit cast is not supported.");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastToUnstructured(
    const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != UNSTRUCTURED);
    auto child = children[0];
    switch (child->dataType.typeID) {
    case BOOL: {
        return UnaryExecFunction<uint8_t, Value, operation::CastToUnstructured>;
    }
    case INT64: {
        return UnaryExecFunction<int64_t, Value, operation::CastToUnstructured>;
    }
    case DOUBLE: {
        return UnaryExecFunction<double_t, Value, operation::CastToUnstructured>;
    }
    case DATE: {
        return UnaryExecFunction<date_t, Value, operation::CastToUnstructured>;
    }
    case TIMESTAMP: {
        return UnaryExecFunction<timestamp_t, Value, operation::CastToUnstructured>;
    }
    case INTERVAL: {
        return UnaryExecFunction<interval_t, Value, operation::CastToUnstructured>;
    }
    case STRING: {
        return castStringToUnstructured;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect UNSTRUCTURED. Implicit cast is not supported.");
    }
}

// TODO(Guodong): make this function reuse string function executor. Also consider to refactor
// ValueVector.addString() interface to make it also works for unstructured value vector.
void VectorCastOperations::castStringToUnstructured(
    const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
    assert(params.size() == 1);
    auto& operand = *params[0];
    result.resetOverflowBuffer();
    auto outValues = (Value*)result.values;
    if (operand.state->isFlat()) {
        auto pos = operand.state->getPositionOfCurrIdx();
        assert(pos == result.state->getPositionOfCurrIdx());
        result.setNull(pos, operand.isNull(pos));
        if (!result.isNull(pos)) {
            ValueVectorUtils::addGFStringToUnstructuredVector(
                result, pos, ((gf_string_t*)operand.values)[pos]);
            outValues[pos].dataType.typeID = STRING;
        }
    } else {
        for (auto i = 0u; i < operand.state->selectedSize; i++) {
            auto pos = operand.state->selectedPositions[i];
            ValueVectorUtils::addGFStringToUnstructuredVector(
                result, pos, ((gf_string_t*)operand.values)[pos]);
            outValues[pos].dataType.typeID = STRING;
        }
    }
}

} // namespace function
} // namespace graphflow
