#pragma once

#include "binary_operation_executor.h"
#include "const_operation_executor.h"
#include "function_definition.h"
#include "ternary_operation_executor.h"
#include "unary_operation_executor.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::common;
using namespace graphflow::binder;

namespace graphflow {
namespace function {

// Forward declaration of VectorOperationDefinition.
struct VectorOperationDefinition;

using scalar_exec_func = std::function<void(const vector<shared_ptr<ValueVector>>&, ValueVector&)>;
using scalar_select_func = std::function<uint64_t(const vector<shared_ptr<ValueVector>>&, sel_t*)>;
using scalar_bind_func =
    std::function<void(const vector<DataType>&, VectorOperationDefinition*, DataType&)>;

struct VectorOperationDefinition : public FunctionDefinition {

    VectorOperationDefinition(string name, vector<DataTypeID> parameterTypeIDs,
        DataTypeID returnTypeID, scalar_exec_func execFunc, bool isVarLength = false)
        : VectorOperationDefinition{move(name), move(parameterTypeIDs), returnTypeID,
              move(execFunc), nullptr, isVarLength} {}

    VectorOperationDefinition(string name, vector<DataTypeID> parameterTypeIDs,
        DataTypeID returnTypeID, scalar_exec_func execFunc, scalar_select_func selectFunc,
        bool isVarLength = false)
        : FunctionDefinition{move(name), move(parameterTypeIDs), returnTypeID}, execFunc{move(
                                                                                    execFunc)},
          selectFunc(move(selectFunc)), isVarLength{isVarLength} {}

    VectorOperationDefinition(string name, vector<DataTypeID> parameterTypeIDs,
        DataTypeID returnTypeID, scalar_exec_func execFunc, scalar_select_func selectFunc,
        scalar_bind_func bindFunc, bool isVarLength = false)
        : FunctionDefinition{move(name), move(parameterTypeIDs), returnTypeID}, execFunc{move(
                                                                                    execFunc)},
          selectFunc(move(selectFunc)), bindFunc{move(bindFunc)}, isVarLength{isVarLength} {}

    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    scalar_bind_func bindFunc;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;
};

class VectorOperations {

public:
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::execute<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static uint64_t BinarySelectFunction(
        const vector<shared_ptr<ValueVector>>& params, sel_t* selectedPositions) {
        assert(params.size() == 2);
        return BinaryOperationExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selectedPositions);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::execute<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void ConstExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 0);
        ConstOperationExecutor::execute<RESULT_TYPE, FUNC>(result);
    }

    template<typename OPERAND_TYPE, typename FUNC>
    static uint64_t UnarySelectFunction(
        const vector<shared_ptr<ValueVector>>& params, sel_t* selectedPositions) {
        assert(params.size() == 1);
        return UnaryOperationExecutor::select<OPERAND_TYPE, FUNC>(*params[0], selectedPositions);
    }
};

} // namespace function
} // namespace graphflow
