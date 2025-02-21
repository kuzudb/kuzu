#pragma once

#include "binary_function_executor.h"
#include "const_function_executor.h"
#include "function.h"
#include "pointer_function_executor.h"
#include "ternary_function_executor.h"
#include "unary_function_executor.h"

namespace kuzu {
namespace function {

// Evaluate function at compile time, e.g. struct_extraction.
using scalar_func_compile_exec_t =
    std::function<void(FunctionBindData*, const std::vector<std::shared_ptr<common::ValueVector>>&,
        std::shared_ptr<common::ValueVector>&)>;
// Execute function.
using scalar_func_exec_t =
    std::function<void(std::span<const common::SelectedVector>, common::SelectedVector, void*)>;
// Execute boolean function and write result to selection vector. Fast path for filter.
using scalar_func_select_t = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&)>;

struct KUZU_API ScalarFunction : public ScalarOrAggregateFunction {
    scalar_func_exec_t execFunc = nullptr;
    scalar_func_select_t selectFunc = nullptr;
    scalar_func_compile_exec_t compileFunc = nullptr;

    ScalarFunction() = default;
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID} {}
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_func_exec_t execFunc)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          execFunc{std::move(execFunc)} {}
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_func_exec_t execFunc,
        scalar_func_select_t selectFunc)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          execFunc{std::move(execFunc)}, selectFunc{std::move(selectFunc)} {}

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryFunctionWrapper>(params[0], params[1], params[2], result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryStringFunctionWrapper>(params[0], params[1], params[2], result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryRegexExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryRegexFunctionWrapper>(params[0], params[1], params[2], result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecListStructFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryListFunctionWrapper>(params[0], params[1], params[2], result, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(params[0],
            params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryStringFunctionWrapper>(params[0], params[1], result, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecListStructFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryListStructFunctionWrapper>(params[0], params[1], result, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecMapCreationFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryMapCreationFunctionWrapper>(params[0], params[1], result, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(*params[0], *params[1],
            selVector);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnarySequenceExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSequence<OPERAND_TYPE, RESULT_TYPE, FUNC>(params[0], result,
            dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStringFunctionWrapper>(params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastStringExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryCastStringFunctionWrapper>(params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastFunctionWrapper>(
            params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecNestedTypeFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryNestedTypeFunctionWrapper>(params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecStructFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStructFunctionWrapper>(params[0], result, dataPtr);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void NullaryExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.empty());
        (void)params;
        ConstFunctionExecutor::execute<RESULT_TYPE, FUNC>(result.vec);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void NullaryAuxilaryExecFunction(std::span<const common::SelectedVector> params,
        common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(params.empty());
        (void)params;
        PointerFunctionExecutor::execute<RESULT_TYPE, FUNC>(result.vec, dataPtr);
    }

    virtual std::unique_ptr<ScalarFunction> copy() const {
        return std::make_unique<ScalarFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu
