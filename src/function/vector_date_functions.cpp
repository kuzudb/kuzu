#include "function/date/vector_date_functions.h"

#include "function/date/date_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions DatePartVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE},
        LogicalTypeID::INT64, BinaryExecFunction<ku_string_t, date_t, int64_t, DatePart>));
    result.push_back(make_unique<VectorFunctionDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::INT64, BinaryExecFunction<ku_string_t, timestamp_t, int64_t, DatePart>));
    result.push_back(make_unique<VectorFunctionDefinition>(DATE_PART_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INTERVAL},
        LogicalTypeID::INT64, BinaryExecFunction<ku_string_t, interval_t, int64_t, DatePart>));
    return result;
}

vector_function_definitions DateTruncVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<ku_string_t, date_t, date_t, DateTrunc>));
    result.push_back(make_unique<VectorFunctionDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<ku_string_t, timestamp_t, timestamp_t, DateTrunc>));
    return result;
}

vector_function_definitions DayNameVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(DAYNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::STRING,
        UnaryExecFunction<date_t, ku_string_t, DayName>));
    result.push_back(make_unique<VectorFunctionDefinition>(DAYNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::STRING,
        UnaryExecFunction<timestamp_t, ku_string_t, DayName>));
    return result;
}

vector_function_definitions GreatestVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(GREATEST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, date_t, date_t, Greatest>));
    result.push_back(make_unique<VectorFunctionDefinition>(GREATEST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, Greatest>));
    return result;
}

vector_function_definitions LastDayVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(LAST_DAY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::DATE,
        UnaryExecFunction<date_t, date_t, LastDay>));
    result.push_back(make_unique<VectorFunctionDefinition>(LAST_DAY_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::DATE,
        UnaryExecFunction<timestamp_t, date_t, LastDay>));
    return result;
}

vector_function_definitions LeastVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(LEAST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, date_t, date_t, Least>));
    result.push_back(make_unique<VectorFunctionDefinition>(LEAST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, Least>));
    return result;
}

vector_function_definitions MakeDateVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(MAKE_DATE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::DATE, TernaryExecFunction<int64_t, int64_t, int64_t, date_t, MakeDate>));
    return result;
}

vector_function_definitions MonthNameVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(MONTHNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::STRING,
        UnaryExecFunction<date_t, ku_string_t, MonthName>));
    result.push_back(make_unique<VectorFunctionDefinition>(MONTHNAME_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::STRING,
        UnaryExecFunction<timestamp_t, ku_string_t, MonthName>));
    return result;
}

} // namespace function
} // namespace kuzu
