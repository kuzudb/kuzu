#include "function/timestamp/vector_timestamp_operations.h"

#include "function/timestamp/timestamp_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_operation_definitions CenturyVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(make_unique<VectorOperationDefinition>(CENTURY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::INT64,
        UnaryExecFunction<timestamp_t, int64_t, operation::Century>));
    return result;
}

vector_operation_definitions EpochMsVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(make_unique<VectorOperationDefinition>(EPOCH_MS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::TIMESTAMP,
        UnaryExecFunction<int64_t, timestamp_t, operation::EpochMs>));
    return result;
}

vector_operation_definitions ToTimestampVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(make_unique<VectorOperationDefinition>(TO_TIMESTAMP_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::TIMESTAMP,
        UnaryExecFunction<int64_t, timestamp_t, operation::ToTimestamp>));
    return result;
}

} // namespace function
} // namespace kuzu
