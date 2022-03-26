#pragma once

#include "src/function/cast/operations/include/cast_operations.h"
#include "src/function/include/vector_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

class VectorCastOperations : public VectorOperations {

public:
    static pair<scalar_exec_func, DataType> bindCastStringToDateExecFunction(
        const expression_vector& children) {
        validateNumParameters(CAST_TO_DATE_FUNCTION_NAME, children.size(), 1);
        validateParameterType(CAST_TO_DATE_FUNCTION_NAME, *children[0], STRING);
        return make_pair(
            UnaryExecFunction<gf_string_t, date_t, operation::CastStringToDate>, DataType(DATE));
    }

    static pair<scalar_exec_func, DataType> bindCastStringToTimestampExecFunction(
        const expression_vector& children) {
        validateNumParameters(CAST_TO_TIMESTAMP_FUNCTION_NAME, children.size(), 1);
        validateParameterType(CAST_TO_TIMESTAMP_FUNCTION_NAME, *children[0], STRING);
        return make_pair(
            UnaryExecFunction<gf_string_t, timestamp_t, operation::CastStringToTimestamp>,
            DataType(TIMESTAMP));
    }

    static pair<scalar_exec_func, DataType> bindCastStringToIntervalExecFunction(
        const expression_vector& children) {
        validateNumParameters(CAST_TO_INTERVAL_FUNCTION_NAME, children.size(), 1);
        validateParameterType(CAST_TO_INTERVAL_FUNCTION_NAME, *children[0], STRING);
        return make_pair(
            UnaryExecFunction<gf_string_t, interval_t, operation::CastStringToInterval>,
            DataType(INTERVAL));
    }

    static pair<scalar_exec_func, DataType> bindCastStructuredToStringExecFunction(
        const expression_vector& children) {
        validateNumParameters(CAST_TO_STRING_FUNCTION_NAME, children.size(), 1);
        return make_pair(castStructuredToString, DataType(STRING));
    }

    static scalar_exec_func bindCastUnstructuredToBoolExecFunction(
        const expression_vector& children) {
        // no need to validate internal function. assert instead.
        assert(children.size() == 1 && children[0]->dataType.typeID == UNSTRUCTURED);
        return UnaryExecFunction<Value, uint8_t, operation::CastUnstructuredToBool>;
    }

    // result contains preallocated Value objects with the structured operand's dataType.
    static void castStructuredToUnstructuredValue(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result);

    // result contains preallocated gf_string_t objects.
    // TODO: make this operation also works for unstructured input
    static void castStructuredToString(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result);
};

} // namespace function
} // namespace graphflow
