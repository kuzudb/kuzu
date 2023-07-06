#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct UnionValueVectorFunction : public VectorFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

struct UnionTagVectorFunction : public VectorFunction {
    static vector_function_definitions getDefinitions();
};

struct UnionExtractVectorFunction : public VectorFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
