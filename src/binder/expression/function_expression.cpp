#include "binder/expression/function_expression.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace binder {

std::string ScalarFunctionExpression::getUniqueName(const std::string& functionName,
    const kuzu::binder::expression_vector& children) {
    auto result = functionName + "(";
    for (auto& child : children) {
        result += child->getUniqueName() + ", ";
    }
    result += ")";
    return result;
}

std::string ScalarFunctionExpression::toStringInternal() const {
    auto result = functionName + "(";
    result += ExpressionUtil::toString(children);
    if (functionName == "CAST") {
        result += ", ";
        result += bindData->resultType->toString();
    }
    result += ")";
    return result;
}

std::string AggregateFunctionExpression::getUniqueName(const std::string& functionName,
    kuzu::binder::expression_vector& children, bool isDistinct) {
    auto result = functionName + "(";
    if (isDistinct) {
        result += "DISTINCT ";
    }
    for (auto& child : children) {
        result += child->getUniqueName() + ", ";
    }
    result += ")";
    return result;
}

std::string AggregateFunctionExpression::toStringInternal() const {
    auto result = functionName + "(";
    if (isDistinct()) {
        result += "DISTINCT ";
    }
    result += ExpressionUtil::toString(children);
    result += ")";
    return result;
}

} // namespace binder
} // namespace kuzu
