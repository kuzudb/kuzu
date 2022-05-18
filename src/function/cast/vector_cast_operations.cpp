#include "include/vector_cast_operations.h"

#include "operations/include/cast_operations.h"

#include "src/common/include/vector/value_vector_utils.h"
#include "src/common/types/include/value.h"
#include "src/function/string/include/vector_string_operations.h"

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
        return UnaryCastExecFunction<Value, gf_string_t, operation::CastToString>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect STRING. Implicit cast is not supported.");
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
        return VectorStringOperations::UnaryStringExecFunction<uint8_t, Value,
            operation::CastToUnstructured>;
    }
    case INT64: {
        return VectorStringOperations::UnaryStringExecFunction<int64_t, Value,
            operation::CastToUnstructured>;
    }
    case DOUBLE: {
        return VectorStringOperations::UnaryStringExecFunction<double_t, Value,
            operation::CastToUnstructured>;
    }
    case DATE: {
        return VectorStringOperations::UnaryStringExecFunction<date_t, Value,
            operation::CastToUnstructured>;
    }
    case TIMESTAMP: {
        return VectorStringOperations::UnaryStringExecFunction<timestamp_t, Value,
            operation::CastToUnstructured>;
    }
    case INTERVAL: {
        return VectorStringOperations::UnaryStringExecFunction<interval_t, Value,
            operation::CastToUnstructured>;
    }
    case STRING: {
        return VectorStringOperations::UnaryStringExecFunction<gf_string_t, Value,
            operation::CastToUnstructured>;
    }
    default:
        throw NotImplementedException("Expression " + child->getRawName() + " has data type " +
                                      Types::dataTypeToString(child->dataType) +
                                      " but expect UNSTRUCTURED. Implicit cast is not supported.");
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
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{BOOL},
            STRING, UnaryCastExecFunction<bool, gf_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{INT64},
            STRING, UnaryCastExecFunction<int64_t, gf_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{DOUBLE},
            STRING, UnaryCastExecFunction<double_t, gf_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{DATE},
            STRING, UnaryCastExecFunction<date_t, gf_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP}, STRING,
        UnaryCastExecFunction<timestamp_t, gf_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        vector<DataTypeID>{INTERVAL}, STRING,
        UnaryCastExecFunction<interval_t, gf_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{STRING},
            STRING, UnaryCastExecFunction<gf_string_t, gf_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{LIST},
            STRING, UnaryCastExecFunction<gf_list_t, gf_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        vector<DataTypeID>{UNSTRUCTURED}, STRING,
        UnaryCastExecFunction<Value, gf_string_t, operation::CastToString>));
    return result;
}

} // namespace function
} // namespace graphflow
