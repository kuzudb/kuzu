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
        for (auto& comparableType : common::LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
            definitions.push_back(getDefinition<FUNC>(name, comparableType, comparableType));
        }
        // We can only check whether two internal ids are equal or not. So INTERNAL_ID is not
        // part of the comparable logical types.
        definitions.push_back(
            getDefinition<FUNC>(name, common::LogicalType{common::LogicalTypeID::INTERNAL_ID},
                common::LogicalType{common::LogicalTypeID::INTERNAL_ID}));
        return definitions;
    }

private:
    template<typename FUNC>
    static inline std::unique_ptr<VectorOperationDefinition> getDefinition(
        const std::string& name, common::LogicalType leftType, common::LogicalType rightType) {
        scalar_exec_func execFunc;
        getExecFunc<FUNC>(leftType.getPhysicalType(), rightType.getPhysicalType(), execFunc);
        scalar_select_func selectFunc;
        getSelectFunc<FUNC>(leftType.getPhysicalType(), rightType.getPhysicalType(), selectFunc);
        return std::make_unique<VectorOperationDefinition>(name,
            std::vector<common::LogicalTypeID>{
                leftType.getLogicalTypeID(), rightType.getLogicalTypeID()},
            common::LogicalTypeID::BOOL, execFunc, selectFunc);
    }

    // When comparing two values, we guarantee that they must have the same dataType. So we only
    // need to switch the physical type to get the corresponding exec function.
    template<typename FUNC>
    static void getExecFunc(
        common::PhysicalTypeID leftType, common::PhysicalTypeID rightType, scalar_exec_func& func) {
        assert(leftType == rightType);
        switch (leftType) {
        case common::PhysicalTypeID::INT64: {
            func = BinaryExecFunction<int64_t, int64_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INT32: {
            func = BinaryExecFunction<int32_t, int32_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INT16: {
            func = BinaryExecFunction<int16_t, int16_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::DOUBLE: {
            func = BinaryExecFunction<double, double, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::FLOAT: {
            func = BinaryExecFunction<float, float, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::BOOL: {
            func = BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::STRING: {
            func = BinaryExecFunction<common::ku_string_t, common::ku_string_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INTERNAL_ID: {
            func = BinaryExecFunction<common::nodeID_t, common::nodeID_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INTERVAL: {
            func = BinaryExecFunction<common::interval_t, common::interval_t, uint8_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::PhysicalTypeUtils::physicalTypeToString(leftType) + "," +
                common::PhysicalTypeUtils::physicalTypeToString(rightType) + ") for getExecFunc.");
        }
    }

    template<typename FUNC>
    static void getSelectFunc(common::PhysicalTypeID leftTypeID, common::PhysicalTypeID rightTypeID,
        scalar_select_func& func) {
        assert(leftTypeID == rightTypeID);
        switch (leftTypeID) {
        case common::PhysicalTypeID::INT64: {
            func = BinarySelectFunction<int64_t, int64_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INT32: {
            func = BinarySelectFunction<int32_t, int32_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INT16: {
            func = BinarySelectFunction<int16_t, int16_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::DOUBLE: {
            func = BinarySelectFunction<double_t, double_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::FLOAT: {
            func = BinarySelectFunction<float_t, float_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::BOOL: {
            func = BinarySelectFunction<uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::STRING: {
            func = BinarySelectFunction<common::ku_string_t, common::ku_string_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INTERNAL_ID: {
            func = BinarySelectFunction<common::nodeID_t, common::nodeID_t, FUNC>;
            return;
        }
        case common::PhysicalTypeID::INTERVAL: {
            func = BinarySelectFunction<common::interval_t, common::interval_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::PhysicalTypeUtils::physicalTypeToString(leftTypeID) + "," +
                common::PhysicalTypeUtils::physicalTypeToString(rightTypeID) +
                ") for getSelectFunc.");
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
