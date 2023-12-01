#include "function/timestamp/vector_timestamp_functions.h"

#include "function/timestamp/timestamp_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set CenturyFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(CENTURY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<timestamp_t, int64_t, Century>));
    return result;
}

function_set EpochMsFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(EPOCH_MS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::TIMESTAMP,
        ScalarFunction::UnaryExecFunction<int64_t, timestamp_t, EpochMs>));
    return result;
}

function_set ToTimestampFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(TO_TIMESTAMP_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE}, LogicalTypeID::TIMESTAMP,
        ScalarFunction::UnaryExecFunction<double_t, timestamp_t, ToTimestamp>));
    return result;
}

} // namespace function
} // namespace kuzu
