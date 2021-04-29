#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    shared_ptr<LogicalExpression> left, shared_ptr<LogicalExpression> right)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(left);
    childrenExpr.push_back(right);
}

LogicalExpression::LogicalExpression(
    ExpressionType expressionType, DataType dataType, shared_ptr<LogicalExpression> child)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(child);
}

LogicalExpression::LogicalExpression(
    ExpressionType expressionType, DataType dataType, const string& variableName)
    : LogicalExpression(expressionType, dataType) {
    this->variableName = variableName;
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
