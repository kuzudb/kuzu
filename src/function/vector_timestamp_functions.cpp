#include "function/timestamp/vector_timestamp_functions.h"

#include "function/timestamp/timestamp_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions CenturyVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(CENTURY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::INT64,
        UnaryExecFunction<timestamp_t, int64_t, Century>));
    return result;
}

vector_function_definitions EpochMsVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(EPOCH_MS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::TIMESTAMP,
        UnaryExecFunction<int64_t, timestamp_t, EpochMs>));
    return result;
}

vector_function_definitions ToTimestampVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(TO_TIMESTAMP_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::TIMESTAMP,
        UnaryExecFunction<int64_t, timestamp_t, ToTimestamp>));
    return result;
}

} // namespace function
} // namespace kuzu
