#include "binder/expression/function_expression.h"

namespace kuzu {
namespace binder {

std::string ScalarFunctionExpression::getUniqueName(
    const std::string& functionName, kuzu::binder::expression_vector& children) {
    auto result = functionName + "(";
    for (auto& child : children) {
        result += child->getUniqueName() + ", ";
    }
    result += ")";
    return result;
}

std::string ScalarFunctionExpression::toString() const {
    auto result = functionName + "(";
    result += ExpressionUtil::toString(children);
    result += ")";
    return result;
}

std::string AggregateFunctionExpression::getUniqueName(
    const std::string& functionName, kuzu::binder::expression_vector& children, bool isDistinct) {
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

std::string AggregateFunctionExpression::toString() const {
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
