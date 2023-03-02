#pragma once

#include "function/vector_operations.h"
#include "interval_operations.h"

namespace kuzu {
namespace function {

class VectorIntervalOperations : public VectorOperations {
public:
    template<class OPERATION>
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>>
    getUnaryIntervalFunctionDefintion(std::string funcName) {
        std::vector<std::unique_ptr<VectorOperationDefinition>> result;
        result.push_back(std::make_unique<VectorOperationDefinition>(funcName,
            std::vector<common::DataTypeID>{common::INT64}, common::INTERVAL,
            UnaryExecFunction<int64_t, common::interval_t, OPERATION>));
        return result;
    }
};

struct ToYearsVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToYears>(
            common::TO_YEARS_FUNC_NAME);
    }
};

struct ToMonthsVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToMonths>(
            common::TO_MONTHS_FUNC_NAME);
    }
};

struct ToDaysVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToDays>(
            common::TO_DAYS_FUNC_NAME);
    }
};

struct ToHoursVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToHours>(
            common::TO_HOURS_FUNC_NAME);
    }
};

struct ToMinutesVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToMinutes>(
            common::TO_MINUTES_FUNC_NAME);
    }
};

struct ToSecondsVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<operation::ToSeconds>(
            common::TO_SECONDS_FUNC_NAME);
    }
};

struct ToMillisecondsVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<
            operation::ToMilliseconds>(common::TO_MILLISECONDS_FUNC_NAME);
    }
};

struct ToMicrosecondsVectorOperation : public VectorIntervalOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorIntervalOperations::getUnaryIntervalFunctionDefintion<
            operation::ToMicroseconds>(common::TO_MICROSECONDS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
