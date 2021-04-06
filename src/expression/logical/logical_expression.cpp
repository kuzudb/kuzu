#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    shared_ptr<LogicalExpression> left, shared_ptr<LogicalExpression> right, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(left);
    childrenExpr.push_back(right);
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    shared_ptr<LogicalExpression> child, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(child);
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    const string& variableName, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    this->variableName = variableName;
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    const Literal& literalValue, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    this->literalValue = literalValue;
    this->rawExpression = move(rawExpression);
}

unordered_set<string> LogicalExpression::getIncludedVariables() const {
    auto result = unordered_set<string>();
    if (isExpressionLeafLiteral(expressionType)) {
        return result;
    }
    if (VARIABLE == expressionType) {
        result.insert(variableName);
        return result;
    }
    if (PROPERTY == expressionType) {
        result.insert(variableName.substr(0, variableName.find('.')));
        return result;
    }
    for (auto& childExpr : childrenExpr) {
        auto tmp = childExpr->getIncludedVariables();
        result.insert(begin(tmp), end(tmp));
    }
    return result;
}

unordered_set<string> LogicalExpression::getIncludedProperties() const {
    auto result = unordered_set<string>();
    if (isExpressionLeafLiteral(expressionType) || VARIABLE == expressionType) {
        return result;
    }
    if (PROPERTY == expressionType) {
        result.insert(variableName);
        return result;
    }
    for (auto& childExpr : childrenExpr) {
        auto tmp = childExpr->getIncludedProperties();
        result.insert(begin(tmp), end(tmp));
    }
    return result;
}

} // namespace expression
} // namespace graphflow
