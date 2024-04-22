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
    rewrite_func_rewrite_t rewriteFunc;

    RewriteFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        rewrite_func_rewrite_t rewriteFunc)
        : Function{name, std::move(parameterTypeIDs)}, rewriteFunc{rewriteFunc} {}
    RewriteFunction(const RewriteFunction& other)
        : Function{other}, rewriteFunc{other.rewriteFunc} {}

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<RewriteFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu
