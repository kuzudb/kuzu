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

unordered_set<string> Expression::getIncludedVariableNames() const {
    unordered_set<string> result;
    for (auto& variableExpression : getIncludedVariableExpressions()) {
        result.insert(variableExpression->getInternalName());
    }
    return result;
}

vector<const Expression*> Expression::getIncludedVariableExpressions() const {
    return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{VARIABLE});
}

vector<const Expression*> Expression::getIncludedPropertyExpressions() const {
    return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{PROPERTY});
}

vector<const Expression*> Expression::getIncludedLeafExpressions() const {
    return getIncludedExpressionsWithTypes(
        unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
}

vector<const Expression*> Expression::getIncludedExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) const {
    auto result = vector<const Expression*>();
    if (expressionTypes.contains(expressionType)) {
        result.push_back(this);
        return result;
    }
    if (expressionType == EXISTENTIAL_SUBQUERY) {
        return getIncludedExpressionsWithTypes(expressionTypes);
    }
    for (auto& child : children) {
        auto tmp = child->getIncludedExpressionsWithTypes(expressionTypes);
        result.insert(end(result), begin(tmp), end(tmp));
    }
    return result;
}

} // namespace binder
} // namespace graphflow
