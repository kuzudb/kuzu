#include "binder/binder_scope.h"

namespace kuzu {
namespace binder {

void BinderScope::addExpression(const std::string& varName,
    std::shared_ptr<Expression> expression) {
    nameToExprIdx.insert({varName, expressions.size()});
    expressions.push_back(std::move(expression));
}

void BinderScope::clear() {
    expressions.clear();
    nameToExprIdx.clear();
}

} // namespace binder
} // namespace kuzu
