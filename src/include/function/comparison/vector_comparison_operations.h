#pragma once

#include "binder/expression/expression.h"
#include "comparison_operations.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorComparisonOperations : public VectorOperations {
protected:
    template<typename FUNC>
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions(
        const std::string& name) {
        std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
        for (auto& numericTypeID : common::LogicalType::getNumericalLogicalTypeIDs()) {
            definitions.push_back(getDefinition<FUNC>(name, numericTypeID, numericTypeID));
        }
        for (auto& typeID : std::vector<common::LogicalTypeID>{common::LogicalTypeID::BOOL,
                 common::LogicalTypeID::STRING, common::LogicalTypeID::INTERNAL_ID,
                 common::LogicalTypeID::DATE, common::LogicalTypeID::TIMESTAMP,
                 common::LogicalTypeID::INTERVAL}) {
            definitions.push_back(getDefinition<FUNC>(name, typeID, typeID));
        }
        definitions.push_back(getDefinition<FUNC>(
            name, common::LogicalTypeID::DATE, common::LogicalTypeID::TIMESTAMP));
        definitions.push_back(getDefinition<FUNC>(
            name, common::LogicalTypeID::TIMESTAMP, common::LogicalTypeID::DATE));
        return definitions;
    }

private:
    template<typename FUNC>
    static inline std::unique_ptr<VectorOperationDefinition> getDefinition(const std::string& name,
        common::LogicalTypeID leftTypeID, common::LogicalTypeID rightTypeID) {
        scalar_exec_func execFunc;
        getExecFunc<FUNC>(leftTypeID, rightTypeID, execFunc);
        scalar_select_func selectFunc;
        getSelectFunc<FUNC>(leftTypeID, rightTypeID, selectFunc);
        return std::make_unique<VectorOperationDefinition>(name,
            std::vector<common::LogicalTypeID>{leftTypeID, rightTypeID},
            common::LogicalTypeID::BOOL, execFunc, selectFunc);
    }

    template<typename FUNC>
    static void getExecFunc(common::LogicalTypeID leftTypeID, common::LogicalTypeID rightTypeID,
        scalar_exec_func& func) {
        switch (leftTypeID) {
        case common::LogicalTypeID::INT64: {
            func = BinaryExecFunction<int64_t, int64_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = BinaryExecFunction<int32_t, int32_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = BinaryExecFunction<int16_t, int16_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = BinaryExecFunction<double, double, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = BinaryExecFunction<float, float, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::BOOL: {
            func = BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::STRING: {
            func = BinaryExecFunction<common::ku_string_t, common::ku_string_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INTERNAL_ID: {
            func = BinaryExecFunction<common::nodeID_t, common::nodeID_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DATE: {
            switch (rightTypeID) {
            case common::LogicalTypeID::DATE: {
                func = BinaryExecFunction<common::date_t, common::date_t, uint8_t, FUNC>;
                return;
            }
            case common::LogicalTypeID::TIMESTAMP: {
                func = BinaryExecFunction<common::date_t, common::timestamp_t, uint8_t, FUNC>;
                return;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" +
                    common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                    common::LogicalTypeUtils::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case common::LogicalTypeID::TIMESTAMP: {
            switch (rightTypeID) {
            case common::LogicalTypeID::DATE: {
                func = BinaryExecFunction<common::timestamp_t, common::date_t, uint8_t, FUNC>;
                return;
            }
            case common::LogicalTypeID::TIMESTAMP: {
                func = BinaryExecFunction<common::timestamp_t, common::timestamp_t, uint8_t, FUNC>;
                return;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" +
                    common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                    common::LogicalTypeUtils::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case common::LogicalTypeID::INTERVAL: {
            func = BinaryExecFunction<common::interval_t, common::interval_t, uint8_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                common::LogicalTypeUtils::dataTypeToString(rightTypeID) + ") for getExecFunc.");
        }
    }

    template<typename FUNC>
    static void getSelectFunc(common::LogicalTypeID leftTypeID, common::LogicalTypeID rightTypeID,
        scalar_select_func& func) {
        switch (leftTypeID) {
        case common::LogicalTypeID::INT64: {
            func = BinarySelectFunction<int64_t, int64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = BinarySelectFunction<int32_t, int32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = BinarySelectFunction<int16_t, int16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = BinarySelectFunction<double_t, double_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = BinarySelectFunction<float_t, float_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::BOOL: {
            func = BinarySelectFunction<uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::STRING: {
            func = BinarySelectFunction<common::ku_string_t, common::ku_string_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INTERNAL_ID: {
            func = BinarySelectFunction<common::nodeID_t, common::nodeID_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DATE: {
            switch (rightTypeID) {
            case common::LogicalTypeID::DATE: {
                func = BinarySelectFunction<common::date_t, common::date_t, FUNC>;
                return;
            }
            case common::LogicalTypeID::TIMESTAMP: {
                func = BinarySelectFunction<common::date_t, common::timestamp_t, FUNC>;
                return;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" +
                    common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                    common::LogicalTypeUtils::dataTypeToString(rightTypeID) +
                    ") for getSelectFunc.");
            }
        }
        case common::LogicalTypeID::TIMESTAMP: {
            switch (rightTypeID) {
            case common::LogicalTypeID::DATE: {
                func = BinarySelectFunction<common::timestamp_t, common::date_t, FUNC>;
                return;
            }
            case common::LogicalTypeID::TIMESTAMP: {
                func = BinarySelectFunction<common::timestamp_t, common::timestamp_t, FUNC>;
                return;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" +
                    common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                    common::LogicalTypeUtils::dataTypeToString(rightTypeID) +
                    ") for getSelectFunc.");
            }
        }
        case common::LogicalTypeID::INTERVAL: {
            func = BinarySelectFunction<common::interval_t, common::interval_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(leftTypeID) + "," +
                common::LogicalTypeUtils::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
        }
    }
};

struct EqualsVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::Equals>(
            common::EQUALS_FUNC_NAME);
    }
};

struct NotEqualsVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::NotEquals>(
            common::NOT_EQUALS_FUNC_NAME);
    }
};

struct GreaterThanVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThan>(
            common::GREATER_THAN_FUNC_NAME);
    }
};

struct GreaterThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThanEquals>(
            common::GREATER_THAN_EQUALS_FUNC_NAME);
    }
};

struct LessThanVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThan>(
            common::LESS_THAN_FUNC_NAME);
    }
};

struct LessThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThanEquals>(
            common::LESS_THAN_EQUALS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
