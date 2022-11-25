#pragma once

#include "function/vector_operations.h"
#include "interval_operations.h"

namespace kuzu {
namespace function {

class VectorIntervalOperations : public VectorOperations {
public:
    template<class OPERATION>
    static inline vector<unique_ptr<VectorOperationDefinition>> getUnaryIntervalFunctionDefintion(
        string funcName) {
        vector<unique_ptr<VectorOperationDefinition>> result;
        result.push_back(make_unique<VectorOperationDefinition>(funcName, vector<DataTypeID>{INT64},
            INTERVAL, UnaryExecFunction<int64_t, interval_t, OPERATION>));
        return result;
    }
};

struct ToYearsVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToYears>(
            TO_YEARS_FUNC_NAME);
    }
};

struct ToMonthsVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToMonths>(
            TO_MONTHS_FUNC_NAME);
    }
};

struct ToDaysVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToDays>(
            TO_DAYS_FUNC_NAME);
    }
};

struct ToHoursVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToHours>(
            TO_HOURS_FUNC_NAME);
    }
};

struct ToMinutesVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToMinutes>(
            TO_MINUTES_FUNC_NAME);
    }
};

struct ToSecondsVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToSeconds>(
            TO_SECONDS_FUNC_NAME);
    }
};

struct ToMillisecondsVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<
            operation::ToMilliseconds>(TO_MILLISECONDS_FUNC_NAME);
    }
};

struct ToMicrosecondsVectorOperation : public VectorIntervalOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<
            operation::ToMicroseconds>(TO_MICROSECONDS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
