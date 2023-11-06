#pragma once

#include "function/scalar_function.h"
#include "interval_functions.h"

namespace kuzu {
namespace function {

struct IntervalFunction {
public:
    template<class OPERATION>
    static inline function_set getUnaryIntervalFunction(std::string funcName) {
        function_set result;
        result.push_back(std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::INT64},
            common::LogicalTypeID::INTERVAL,
            ScalarFunction::UnaryExecFunction<int64_t, common::interval_t, OPERATION>));
        return result;
    }
};

struct ToYearsFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToYears>(common::TO_YEARS_FUNC_NAME);
    }
};

struct ToMonthsFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToMonths>(common::TO_MONTHS_FUNC_NAME);
    }
};

struct ToDaysFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToDays>(common::TO_DAYS_FUNC_NAME);
    }
};

struct ToHoursFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToHours>(common::TO_HOURS_FUNC_NAME);
    }
};

struct ToMinutesFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToMinutes>(common::TO_MINUTES_FUNC_NAME);
    }
};

struct ToSecondsFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToSeconds>(common::TO_SECONDS_FUNC_NAME);
    }
};

struct ToMillisecondsFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToMilliseconds>(
            common::TO_MILLISECONDS_FUNC_NAME);
    }
};

struct ToMicrosecondsFunction {
    static inline function_set getFunctionSet() {
        return IntervalFunction::getUnaryIntervalFunction<ToMicroseconds>(
            common::TO_MICROSECONDS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
