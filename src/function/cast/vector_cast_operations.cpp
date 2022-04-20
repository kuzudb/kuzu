#include "include/vector_cast_operations.h"

#include "operations/include/cast_operations.h"

#include "src/common/include/vector/value_vector_utils.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace function {

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

scalar_exec_func VectorCastOperations::bindImplicitCastToInt64(const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != INT64);
    auto child = children[0];
    switch (children[0]->dataType.typeID) {
    case UNSTRUCTURED: {
        return UnaryExecFunction<Value, int64_t, operation::CastUnstructuredToInt64>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect INT64. Implicit cast is not supported.");
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

scalar_exec_func VectorCastOperations::bindImplicitCastToDate(const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != DATE);
    auto child = children[0];
    switch (child->dataType.typeID) {
    case UNSTRUCTURED: {
        return UnaryExecFunction<Value, date_t, operation::CastUnstructuredToDate>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect DATE. Implicit cast is not supported.");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastToTimestamp(
    const expression_vector& children) {
    assert(children.size() == 1 && children[0]->dataType.typeID != TIMESTAMP);
    auto child = children[0];
    switch (child->dataType.typeID) {
    case UNSTRUCTURED: {
        return UnaryExecFunction<Value, timestamp_t, operation::CastUnstructuredToTimestamp>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect TIMESTAMP. Implicit cast is not supported.");
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

vector<unique_ptr<VectorOperationDefinition>> CastToDateVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_DATE_FUNC_NAME, vector<DataTypeID>{STRING},
            DATE, UnaryExecFunction<gf_string_t, date_t, operation::CastStringToDate>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToTimestampVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_TIMESTAMP_FUNC_NAME,
        vector<DataTypeID>{STRING}, TIMESTAMP,
        UnaryExecFunction<gf_string_t, timestamp_t, operation::CastStringToTimestamp>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToIntervalVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INTERVAL_FUNC_NAME,
        vector<DataTypeID>{STRING}, INTERVAL,
        UnaryExecFunction<gf_string_t, interval_t, operation::CastStringToInterval>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToStringVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    auto functionName = CAST_TO_STRING_FUNC_NAME;
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{BOOL}, STRING, castToString<bool>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{INT64}, STRING, castToString<int64_t>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{DOUBLE}, STRING, castToString<double_t>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{DATE}, STRING, castToString<date_t>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{TIMESTAMP}, STRING, castToString<timestamp_t>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{INTERVAL}, STRING, castToString<interval_t>));
    result.push_back(make_unique<VectorOperationDefinition>(
        functionName, vector<DataTypeID>{UNSTRUCTURED}, STRING, castToString<Value>));
    return result;
}

} // namespace function
} // namespace graphflow
