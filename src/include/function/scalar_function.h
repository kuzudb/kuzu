#pragma once

#include "binary_function_executor.h"
#include "const_function_executor.h"
#include "function.h"
#include "ternary_function_executor.h"
#include "unary_function_executor.h"

namespace kuzu {
namespace function {

struct ScalarFunction;

using scalar_compile_func =
    std::function<void(FunctionBindData*, const std::vector<std::shared_ptr<common::ValueVector>>&,
        std::shared_ptr<common::ValueVector>&)>;
using scalar_exec_func = std::function<void(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::ValueVector&, void*)>;
using scalar_select_func = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&)>;
using function_set = std::vector<std::unique_ptr<Function>>;

struct ScalarFunction final : public BaseScalarFunction {

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), nullptr, nullptr, nullptr, isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, nullptr, isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_bind_func bindFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, std::move(bindFunc),
              isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_compile_func compileFunc, scalar_bind_func bindFunc,
        bool isVarLength = false)
        : BaseScalarFunction{FunctionType::SCALAR, std::move(name), std::move(parameterTypeIDs),
              returnTypeID, std::move(bindFunc)},
          execFunc{std::move(execFunc)}, selectFunc(std::move(selectFunc)),
          compileFunc{std::move(compileFunc)}, isVarLength{isVarLength} {}

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::execute<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeString<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeString<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selVector);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            *params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStringFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryCastStringFunctionWrapper>(*params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastFunctionWrapper>(
            *params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryTryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryTryCastFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryListFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void ConstExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.empty());
        (void)params;
        ConstFunctionExecutor::execute<RESULT_TYPE, FUNC>(result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeListStruct<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeListStruct<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<ScalarFunction>(*this);
    }

    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    scalar_compile_func compileFunc;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;
};

} // namespace function
} // namespace kuzu
