#include "function/date/vector_date_operations.h"

#include "function/date/date_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>> DatePartVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE},
        LogicalTypeID::INT64,
        BinaryExecFunction<ku_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::INT64,
        BinaryExecFunction<ku_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INTERVAL},
        LogicalTypeID::INT64,
        BinaryExecFunction<ku_string_t, interval_t, int64_t, operation::DatePart>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DateTruncVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<ku_string_t, date_t, date_t, operation::DateTrunc>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<ku_string_t, timestamp_t, timestamp_t, operation::DateTrunc>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DayNameVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::STRING,
        UnaryExecFunction<date_t, ku_string_t, operation::DayName>));
    result.push_back(make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::STRING,
        UnaryExecFunction<timestamp_t, ku_string_t, operation::DayName>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> GreatestVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, date_t, date_t, operation::Greatest>));
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Greatest>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LastDayVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::DATE,
        UnaryExecFunction<date_t, date_t, operation::LastDay>));
    result.push_back(make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::DATE,
        UnaryExecFunction<timestamp_t, date_t, operation::LastDay>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LeastVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, date_t, date_t, operation::Least>));
    result.push_back(make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Least>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MakeDateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(MAKE_DATE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::DATE,
        TernaryExecFunction<int64_t, int64_t, int64_t, date_t, operation::MakeDate>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MonthNameVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::STRING,
        UnaryExecFunction<date_t, ku_string_t, operation::MonthName>));
    result.push_back(make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::STRING,
        UnaryExecFunction<timestamp_t, ku_string_t, operation::MonthName>));
    return result;
}

} // namespace function
} // namespace kuzu
