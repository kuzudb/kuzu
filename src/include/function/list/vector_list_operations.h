#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorListOperations : public VectorOperations {
    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFunc(common::LogicalType rightType) {
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
                "VectorListOperations::getBinaryListOperationDefinition"};
        }
        }
        return execFunc;
    }
};

struct ListCreationVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct ListLenVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
};

struct ListExtractVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListConcatVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListAppendVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPrependVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPositionVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListContainsVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListSliceVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListSortVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListReverseSortVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListSumVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListDistinctVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListUniqueVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListAnyValueVectorOperation : public VectorListOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
