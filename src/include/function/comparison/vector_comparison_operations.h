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
        for (auto& numericTypeID : common::DataType::getNumericalTypeIDs()) {
            definitions.push_back(getDefinition<FUNC>(name, numericTypeID, numericTypeID));
        }
        for (auto& typeID : std::vector<common::DataTypeID>{common::BOOL, common::STRING,
                 common::INTERNAL_ID, common::DATE, common::TIMESTAMP, common::INTERVAL}) {
            definitions.push_back(getDefinition<FUNC>(name, typeID, typeID));
        }
        definitions.push_back(getDefinition<FUNC>(name, common::DATE, common::TIMESTAMP));
        definitions.push_back(getDefinition<FUNC>(name, common::TIMESTAMP, common::DATE));
        return definitions;
    }

private:
    template<typename FUNC>
    static inline std::unique_ptr<VectorOperationDefinition> getDefinition(
        const std::string& name, common::DataTypeID leftTypeID, common::DataTypeID rightTypeID) {
        auto execFunc = getExecFunc<FUNC>(leftTypeID, rightTypeID);
        auto selectFunc = getSelectFunc<FUNC>(leftTypeID, rightTypeID);
        return std::make_unique<VectorOperationDefinition>(name,
            std::vector<common::DataTypeID>{leftTypeID, rightTypeID}, common::BOOL, execFunc,
            selectFunc);
    }

    template<typename FUNC>
    static scalar_exec_func getExecFunc(
        common::DataTypeID leftTypeID, common::DataTypeID rightTypeID) {
        switch (leftTypeID) {
        case common::INT64: {
            return BinaryExecFunction<int64_t, int64_t, uint8_t, FUNC>;
        }
        case common::INT32: {
            return BinaryExecFunction<int32_t, int32_t, uint8_t, FUNC>;
        }
        case common::INT16: {
            return BinaryExecFunction<int16_t, int16_t, uint8_t, FUNC>;
        }
        case common::DOUBLE: {
            return BinaryExecFunction<double, double, uint8_t, FUNC>;
        }
        case common::FLOAT: {
            return BinaryExecFunction<float, float, uint8_t, FUNC>;
        }
        case common::BOOL: {
            assert(rightTypeID == common::BOOL);
            return BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
        }
        case common::STRING: {
            assert(rightTypeID == common::STRING);
            return BinaryExecFunction<common::ku_string_t, common::ku_string_t, uint8_t, FUNC>;
        }
        case common::INTERNAL_ID: {
            assert(rightTypeID == common::INTERNAL_ID);
            return BinaryExecFunction<common::nodeID_t, common::nodeID_t, uint8_t, FUNC>;
        }
        case common::DATE: {
            switch (rightTypeID) {
            case common::DATE: {
                return BinaryExecFunction<common::date_t, common::date_t, uint8_t, FUNC>;
            }
            case common::TIMESTAMP: {
                return BinaryExecFunction<common::date_t, common::timestamp_t, uint8_t, FUNC>;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) +
                    "," + common::Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case common::TIMESTAMP: {
            switch (rightTypeID) {
            case common::DATE: {
                return BinaryExecFunction<common::timestamp_t, common::date_t, uint8_t, FUNC>;
            }
            case common::TIMESTAMP: {
                return BinaryExecFunction<common::timestamp_t, common::timestamp_t, uint8_t, FUNC>;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) +
                    "," + common::Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
            }
        }
        case common::INTERVAL: {
            assert(rightTypeID == common::INTERVAL);
            return BinaryExecFunction<common::interval_t, common::interval_t, uint8_t, FUNC>;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) + "," +
                common::Types::dataTypeToString(rightTypeID) + ") for getExecFunc.");
        }
    }

    template<typename FUNC>
    static scalar_select_func getSelectFunc(
        common::DataTypeID leftTypeID, common::DataTypeID rightTypeID) {
        switch (leftTypeID) {
        case common::INT64: {
            return BinarySelectFunction<int64_t, int64_t, FUNC>;
        }
        case common::INT32: {
            return BinarySelectFunction<int32_t, int32_t, FUNC>;
        }
        case common::INT16: {
            return BinarySelectFunction<int16_t, int16_t, FUNC>;
        }
        case common::DOUBLE: {
            return BinarySelectFunction<double_t, double_t, FUNC>;
        }
        case common::FLOAT: {
            return BinarySelectFunction<float_t, float_t, FUNC>;
        }
        case common::BOOL: {
            assert(rightTypeID == common::BOOL);
            return BinarySelectFunction<uint8_t, uint8_t, FUNC>;
        }
        case common::STRING: {
            assert(rightTypeID == common::STRING);
            return BinarySelectFunction<common::ku_string_t, common::ku_string_t, FUNC>;
        }
        case common::INTERNAL_ID: {
            assert(rightTypeID == common::INTERNAL_ID);
            return BinarySelectFunction<common::nodeID_t, common::nodeID_t, FUNC>;
        }
        case common::DATE: {
            switch (rightTypeID) {
            case common::DATE: {
                return BinarySelectFunction<common::date_t, common::date_t, FUNC>;
            }
            case common::TIMESTAMP: {
                return BinarySelectFunction<common::date_t, common::timestamp_t, FUNC>;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) +
                    "," + common::Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case common::TIMESTAMP: {
            switch (rightTypeID) {
            case common::DATE: {
                return BinarySelectFunction<common::timestamp_t, common::date_t, FUNC>;
            }
            case common::TIMESTAMP: {
                return BinarySelectFunction<common::timestamp_t, common::timestamp_t, FUNC>;
            }
            default:
                throw common::RuntimeException(
                    "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) +
                    "," + common::Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
            }
        }
        case common::INTERVAL: {
            assert(rightTypeID == common::INTERVAL);
            return BinarySelectFunction<common::interval_t, common::interval_t, FUNC>;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) + "," +
                common::Types::dataTypeToString(rightTypeID) + ") for getSelectFunc.");
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
