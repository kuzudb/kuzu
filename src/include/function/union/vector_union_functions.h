#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct UnionValueFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

struct UnionTagFunction {
    static function_set getFunctionSet();
};

struct UnionExtractFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
