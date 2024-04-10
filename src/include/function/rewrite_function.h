#pragma once

#include "function.h"

namespace kuzu {
namespace binder {
class ExpressionBinder;
}
namespace function {

// Rewrite function to a different expression, e.g. id(n) -> n._id.
using rewrite_func_rewrite_t = std::function<std::shared_ptr<binder::Expression>(
    const binder::expression_vector&, binder::ExpressionBinder*)>;

// We write for the following functions
// ID(n) -> n._id
struct RewriteFunction final : public Function {

    RewriteFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        rewrite_func_rewrite_t rewriteFunc)
        : Function{FunctionType::REWRITE, name, std::move(parameterTypeIDs)},
          rewriteFunc{rewriteFunc} {}

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<RewriteFunction>(*this);
    }

    rewrite_func_rewrite_t rewriteFunc;
};

} // namespace function
} // namespace kuzu
