#pragma once

#include "binary_operation_executor.h"
#include "binder/expression/expression.h"
#include "const_operation_executor.h"
#include "function_definition.h"
#include "ternary_operation_executor.h"
#include "unary_operation_executor.h"

namespace kuzu {
namespace function {

// Forward declaration of VectorOperationDefinition.
struct VectorOperationDefinition;

using scalar_exec_func = std::function<void(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::ValueVector&)>;
using scalar_select_func = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&)>;

struct VectorOperationDefinition : public FunctionDefinition {

    VectorOperationDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID, scalar_exec_func execFunc, bool isVarLength = false)
        : VectorOperationDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), nullptr, isVarLength} {}

    VectorOperationDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID, scalar_exec_func execFunc, scalar_select_func selectFunc,
        bool isVarLength = false)
        : FunctionDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          execFunc{std::move(execFunc)},
          selectFunc(std::move(selectFunc)), isVarLength{isVarLength} {}

    VectorOperationDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID, scalar_exec_func execFunc, scalar_select_func selectFunc,
        scalar_bind_func bindFunc, bool isVarLength = false)
        : FunctionDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(bindFunc)},
          execFunc{std::move(execFunc)},
          selectFunc(std::move(selectFunc)), isVarLength{isVarLength} {}

    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;
};

class VectorOperations {

public:
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
        assert(params.size() == 0);
        ConstOperationExecutor::execute<RESULT_TYPE, FUNC>(result);
    }
};

} // namespace function
} // namespace kuzu
