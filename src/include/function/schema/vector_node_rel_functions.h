#pragma once

#include "function/vector_functions.h"
#include "offset_functions.h"

namespace kuzu {
namespace function {

struct OffsetVectorFunction {
    static vector_function_definitions getDefinitions();
    static void execFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryFunctionExecutor::execute<common::internalID_t, int64_t, Offset>(*params[0], result);
    }
};

struct NodesVectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct RelsVectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
