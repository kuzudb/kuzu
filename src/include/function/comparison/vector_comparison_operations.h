#pragma once

#include "binder/expression/expression.h"
#include "comparison_operations.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorComparisonOperations : public VectorOperations {
protected:
    template<typename FUNC>
    static vector_operation_definitions getDefinitions(const std::string& name) {
        vector_operation_definitions definitions;
        for (auto& comparableType : common::LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
            definitions.push_back(getDefinition<FUNC>(name, comparableType, comparableType));
        }
        definitions.push_back(
            getDefinition<FUNC>(name, common::LogicalType{common::LogicalTypeID::VAR_LIST},
                common::LogicalType{common::LogicalTypeID::VAR_LIST}));
        definitions.push_back(
            getDefinition<FUNC>(name, common::LogicalType{common::LogicalTypeID::STRUCT},
                common::LogicalType{common::LogicalTypeID::STRUCT}));
        // We can only check whether two internal ids are equal or not. So INTERNAL_ID is not
        // part of the comparable logical types.
        definitions.push_back(
            getDefinition<FUNC>(name, common::LogicalType{common::LogicalTypeID::INTERNAL_ID},
                common::LogicalType{common::LogicalTypeID::INTERNAL_ID}));
        return definitions;
    }

private:
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryComparisonExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeComparison<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinaryComparisonSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        assert(params.size() == 2);
        return BinaryOperationExecutor::selectComparison<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selVector);
    }

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
        switch (leftType) {
        case common::PhysicalTypeID::INT64: {
            func = BinaryComparisonExecFunction<int64_t, int64_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT32: {
            func = BinaryComparisonExecFunction<int32_t, int32_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT16: {
            func = BinaryComparisonExecFunction<int16_t, int16_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::DOUBLE: {
            func = BinaryComparisonExecFunction<double, double, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::FLOAT: {
            func = BinaryComparisonExecFunction<float, float, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::BOOL: {
            func = BinaryComparisonExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::STRING: {
            func = BinaryComparisonExecFunction<common::ku_string_t, common::ku_string_t, uint8_t,
                FUNC>;
        } break;
        case common::PhysicalTypeID::INTERNAL_ID: {
            func = BinaryComparisonExecFunction<common::nodeID_t, common::nodeID_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INTERVAL: {
            func =
                BinaryComparisonExecFunction<common::interval_t, common::interval_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::VAR_LIST: {
            func = BinaryComparisonExecFunction<common::list_entry_t, common::list_entry_t, uint8_t,
                FUNC>;
        } break;
        case common::PhysicalTypeID::STRUCT: {
            func = BinaryComparisonExecFunction<common::struct_entry_t, common::struct_entry_t,
                uint8_t, FUNC>;
        } break;
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
            func = BinaryComparisonSelectFunction<int64_t, int64_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT32: {
            func = BinaryComparisonSelectFunction<int32_t, int32_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT16: {
            func = BinaryComparisonSelectFunction<int16_t, int16_t, FUNC>;
        } break;
        case common::PhysicalTypeID::DOUBLE: {
            func = BinaryComparisonSelectFunction<double_t, double_t, FUNC>;
        } break;
        case common::PhysicalTypeID::FLOAT: {
            func = BinaryComparisonSelectFunction<float_t, float_t, FUNC>;
        } break;
        case common::PhysicalTypeID::BOOL: {
            func = BinaryComparisonSelectFunction<uint8_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::STRING: {
            func = BinaryComparisonSelectFunction<common::ku_string_t, common::ku_string_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INTERNAL_ID: {
            func = BinaryComparisonSelectFunction<common::nodeID_t, common::nodeID_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INTERVAL: {
            func = BinaryComparisonSelectFunction<common::interval_t, common::interval_t, FUNC>;
        } break;
        case common::PhysicalTypeID::VAR_LIST: {
            func = BinaryComparisonSelectFunction<common::list_entry_t, common::list_entry_t, FUNC>;
        } break;
        case common::PhysicalTypeID::STRUCT: {
            func = BinaryComparisonSelectFunction<common::struct_entry_t, common::struct_entry_t,
                FUNC>;
        } break;
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
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::Equals>(
            common::EQUALS_FUNC_NAME);
    }
};

struct NotEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::NotEquals>(
            common::NOT_EQUALS_FUNC_NAME);
    }
};

struct GreaterThanVectorOperation : public VectorComparisonOperations {
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThan>(
            common::GREATER_THAN_FUNC_NAME);
    }
};

struct GreaterThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::GreaterThanEquals>(
            common::GREATER_THAN_EQUALS_FUNC_NAME);
    }
};

struct LessThanVectorOperation : public VectorComparisonOperations {
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThan>(
            common::LESS_THAN_FUNC_NAME);
    }
};

struct LessThanEqualsVectorOperation : public VectorComparisonOperations {
    static inline vector_operation_definitions getDefinitions() {
        return VectorComparisonOperations::getDefinitions<operation::LessThanEquals>(
            common::LESS_THAN_EQUALS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
