#pragma once

#include "function/vector_functions.h"
#include "interval_functions.h"

namespace kuzu {
namespace function {

class VectorIntervalFunction : public VectorFunction {
public:
    template<class OPERATION>
    static inline vector_function_definitions getUnaryIntervalFunctionDefintion(
        std::string funcName) {
        vector_function_definitions result;
        result.push_back(std::make_unique<VectorFunctionDefinition>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::INT64},
            common::LogicalTypeID::INTERVAL,
            UnaryExecFunction<int64_t, common::interval_t, OPERATION>));
        return result;
    }
};

struct ToYearsVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToYears>(
            common::TO_YEARS_FUNC_NAME);
    }
};

struct ToMonthsVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMonths>(
            common::TO_MONTHS_FUNC_NAME);
    }
};

struct ToDaysVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToDays>(
            common::TO_DAYS_FUNC_NAME);
    }
};

struct ToHoursVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToHours>(
            common::TO_HOURS_FUNC_NAME);
    }
};

struct ToMinutesVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMinutes>(
            common::TO_MINUTES_FUNC_NAME);
    }
};

struct ToSecondsVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToSeconds>(
            common::TO_SECONDS_FUNC_NAME);
    }
};

struct ToMillisecondsVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMilliseconds>(
            common::TO_MILLISECONDS_FUNC_NAME);
    }
};

struct ToMicrosecondsVectorFunction : public VectorIntervalFunction {
    static inline vector_function_definitions getDefinitions() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMicroseconds>(
            common::TO_MICROSECONDS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
