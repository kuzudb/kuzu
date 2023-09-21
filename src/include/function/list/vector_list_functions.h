#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct VectorListFunction : public VectorFunction {
    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchRight(common::LogicalType rightType) {
        scalar_exec_func execFunc;
        switch (rightType.getPhysicalType()) {
        case common::PhysicalTypeID::BOOL: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, uint8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT64: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT64: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint64_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT32: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint32_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT16: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint16_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT8: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, uint8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::DOUBLE: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, double_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::FLOAT: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, float_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRING: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::ku_string_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERVAL: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::interval_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERNAL_ID: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::internalID_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::VAR_LIST: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::list_entry_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRUCT: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::struct_entry_t,
                RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getBinaryListOperationDefinition"};
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc = BinaryExecListStructFunction<int64_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc = BinaryExecListStructFunction<int32_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc = BinaryExecListStructFunction<int16_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc = BinaryExecListStructFunction<int8_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getBinaryListOperationDefinition"};
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getTernaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc =
                TernaryExecListStructFunction<int64_t, int64_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc =
                TernaryExecListStructFunction<int32_t, int32_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc =
                TernaryExecListStructFunction<int16_t, int16_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc =
                TernaryExecListStructFunction<int8_t, int8_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getTernaryListOperationDefinition"};
        }
        }
        return execFunc;
    }
};

struct ListCreationVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct ListRangeVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListLenVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
};

struct ListExtractVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListConcatVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListAppendVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPrependVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPositionVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListContainsVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListSliceVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListSortVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListReverseSortVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListSumVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListDistinctVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListUniqueVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListAnyValueVectorFunction : public VectorListFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
