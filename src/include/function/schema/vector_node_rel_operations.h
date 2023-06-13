#pragma once

#include "function/vector_operations.h"
#include "offset_operations.h"

namespace kuzu {
namespace function {

struct OffsetVectorOperation {
    static vector_operation_definitions getDefinitions();
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::execute<common::internalID_t, int64_t, operation::Offset>(
            *params[0], result);
    }
};

struct NodesVectorOperation {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct RelsVectorOperation {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
