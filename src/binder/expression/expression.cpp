#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

Expression::Expression(ExpressionType expressionType, DataType dataType,
    const shared_ptr<Expression>& left, const shared_ptr<Expression>& right)
    : Expression(expressionType, dataType) {
    children.push_back(left);
    children.push_back(right);
}

Expression::Expression(
    ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& child)
    : Expression(expressionType, dataType) {
    children.push_back(child);
}

unordered_set<string> Expression::getDependentVariableNames() {
    unordered_set<string> result;
    for (auto& variableExpression : getDependentVariables()) {
        result.insert(variableExpression->getInternalName());
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getDependentExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) {
    vector<shared_ptr<Expression>> result;
    if (expressionTypes.contains(expressionType)) {
        result.push_back(shared_from_this());
        return result;
    }
    for (auto& child : children) {
        for (auto& expression : child->getDependentExpressionsWithTypes(expressionTypes)) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
