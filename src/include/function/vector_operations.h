#pragma once

#include "binary_operation_executor.h"
#include "binder/expression/expression.h"
#include "const_operation_executor.h"
#include "function_definition.h"
#include "ternary_operation_executor.h"
#include "unary_operation_executor.h"

namespace kuzu {
namespace function {

struct VectorOperationDefinition;

using scalar_compile_func =
    std::function<void(FunctionBindData*, const std::vector<std::shared_ptr<common::ValueVector>>&,
        std::shared_ptr<common::ValueVector>&)>;
using scalar_exec_func = std::function<void(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::ValueVector&)>;
using scalar_select_func = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&)>;
using vector_operation_definitions = std::vector<std::unique_ptr<VectorOperationDefinition>>;

struct VectorOperationDefinition : public FunctionDefinition {

    VectorOperationDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc, bool isVarLength = false)
        : VectorOperationDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), nullptr, nullptr, nullptr, isVarLength} {}

    VectorOperationDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, bool isVarLength = false)
        : VectorOperationDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, nullptr, isVarLength} {}

    VectorOperationDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_bind_func bindFunc, bool isVarLength = false)
        : VectorOperationDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, std::move(bindFunc),
              isVarLength} {}

    VectorOperationDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_compile_func compileFunc, scalar_bind_func bindFunc,
        bool isVarLength = false)
        : FunctionDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(bindFunc)},
          execFunc{std::move(execFunc)}, selectFunc(std::move(selectFunc)),
          compileFunc{std::move(compileFunc)}, isVarLength{isVarLength} {}

    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    scalar_compile_func compileFunc;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;
};

struct VectorOperations {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::execute<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        assert(params.size() == 2);
        return BinaryOperationExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selVector);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::execute<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void ConstExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.empty());
        ConstOperationExecutor::execute<RESULT_TYPE, FUNC>(result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::executeListStruct<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeListStruct<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeListStruct<OPERAND_TYPE, RESULT_TYPE, FUNC>(
            *params[0], result);
    }
};

} // namespace function
} // namespace kuzu
