#pragma once

#include <cmath>

#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "comparison_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct ComparisonFunction {
    template<typename OP>
    static function_set getFunctionSet(const std::string& name) {
        function_set functionSet;
        for (auto& comparableType : common::LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
            functionSet.push_back(getFunction<OP>(name, comparableType, comparableType));
        }
        functionSet.push_back(
            getFunction<OP>(name, common::LogicalType{common::LogicalTypeID::VAR_LIST},
                common::LogicalType{common::LogicalTypeID::VAR_LIST}));
        functionSet.push_back(
            getFunction<OP>(name, common::LogicalType{common::LogicalTypeID::STRUCT},
                common::LogicalType{common::LogicalTypeID::STRUCT}));
        // We can only check whether two internal ids are equal or not. So INTERNAL_ID is not
        // part of the comparable logical types.
        functionSet.push_back(
            getFunction<OP>(name, common::LogicalType{common::LogicalTypeID::INTERNAL_ID},
                common::LogicalType{common::LogicalTypeID::INTERNAL_ID}));
        return functionSet;
    }

private:
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryComparisonExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeComparison<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinaryComparisonSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::selectComparison<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selVector);
    }

    template<typename FUNC>
    static inline std::unique_ptr<ScalarFunction> getFunction(
        const std::string& name, common::LogicalType leftType, common::LogicalType rightType) {
        scalar_exec_func execFunc;
        getExecFunc<FUNC>(leftType.getPhysicalType(), rightType.getPhysicalType(), execFunc);
        scalar_select_func selectFunc;
        getSelectFunc<FUNC>(leftType.getPhysicalType(), rightType.getPhysicalType(), selectFunc);
        return std::make_unique<ScalarFunction>(name,
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
        case common::PhysicalTypeID::INT8: {
            func = BinaryComparisonExecFunction<int8_t, int8_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT64: {
            func = BinaryComparisonExecFunction<uint64_t, uint64_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT32: {
            func = BinaryComparisonExecFunction<uint32_t, uint32_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT16: {
            func = BinaryComparisonExecFunction<uint16_t, uint16_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT8: {
            func = BinaryComparisonExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT128: {
            func = BinaryComparisonExecFunction<common::int128_t, common::int128_t, uint8_t, FUNC>;
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
        KU_ASSERT(leftTypeID == rightTypeID);
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
        case common::PhysicalTypeID::INT8: {
            func = BinaryComparisonSelectFunction<int8_t, int8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT64: {
            func = BinaryComparisonSelectFunction<uint64_t, uint64_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT32: {
            func = BinaryComparisonSelectFunction<uint32_t, uint32_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT16: {
            func = BinaryComparisonSelectFunction<uint16_t, uint16_t, FUNC>;
        } break;
        case common::PhysicalTypeID::UINT8: {
            func = BinaryComparisonSelectFunction<uint8_t, uint8_t, FUNC>;
        } break;
        case common::PhysicalTypeID::INT128: {
            func = BinaryComparisonSelectFunction<common::int128_t, common::int128_t, FUNC>;
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

struct EqualsFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<Equals>(common::EQUALS_FUNC_NAME);
    }
};

struct NotEqualsFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<NotEquals>(common::NOT_EQUALS_FUNC_NAME);
    }
};

struct GreaterThanFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<GreaterThan>(common::GREATER_THAN_FUNC_NAME);
    }
};

struct GreaterThanEqualsFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<GreaterThanEquals>(
            common::GREATER_THAN_EQUALS_FUNC_NAME);
    }
};

struct LessThanFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<LessThan>(common::LESS_THAN_FUNC_NAME);
    }
};

struct LessThanEqualsFunction {
    static inline function_set getFunctionSet() {
        return ComparisonFunction::getFunctionSet<LessThanEquals>(
            common::LESS_THAN_EQUALS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
