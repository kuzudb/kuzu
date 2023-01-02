#include "function/timestamp/vector_timestamp_operations.h"

#include "function/timestamp/timestamp_operations.h"

namespace kuzu {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> CenturyVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(CENTURY_FUNC_NAME, vector<DataTypeID>{TIMESTAMP},
            INT64, UnaryExecFunction<timestamp_t, int64_t, operation::Century>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> EpochMsVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(EPOCH_MS_FUNC_NAME, vector<DataTypeID>{INT64},
            TIMESTAMP, UnaryExecFunction<int64_t, timestamp_t, operation::EpochMs>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> ToTimestampVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(TO_TIMESTAMP_FUNC_NAME, vector<DataTypeID>{INT64},
            TIMESTAMP, UnaryExecFunction<int64_t, timestamp_t, operation::ToTimestamp>));
    return result;
}

} // namespace function
} // namespace kuzu
