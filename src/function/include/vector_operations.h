#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

using namespace graphflow::common;
using namespace graphflow::binder;

namespace graphflow {
namespace function {

using scalar_exec_func = std::function<void(const vector<shared_ptr<ValueVector>>&, ValueVector&)>;
using scalar_select_func = std::function<uint64_t(const vector<shared_ptr<ValueVector>>&, sel_t*)>;

class VectorOperations {

protected:
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

    template<typename OPERAND_TYPE, typename FUNC>
    static uint64_t UnarySelectFunction(
        const vector<shared_ptr<ValueVector>>& params, sel_t* selectedPositions) {
        assert(params.size() == 1);
        return UnaryOperationExecutor::select<OPERAND_TYPE, FUNC>(*params[0], selectedPositions);
    }
};

} // namespace function
} // namespace graphflow
