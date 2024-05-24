#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct DecimalFunction {

    static std::unique_ptr<FunctionBindData> bindAddFunc(const binder::expression_vector& arguments,
        Function*);

    static std::unique_ptr<FunctionBindData> bindSubtractFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindMultiplyFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindDivideFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindModuloFunc(
        const binder::expression_vector& arguments, Function*);
};

} // namespace function
} // namespace kuzu
