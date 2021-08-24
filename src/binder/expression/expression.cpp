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

bool Expression::hasSubqueryExpression() const {
    auto result = false;
    if (expressionType == EXISTENTIAL_SUBQUERY) {
        result = true;
    } else {
        for (auto& child : children) {
            result |= child->hasSubqueryExpression();
        }
    }
    return result;
}

unordered_set<string> Expression::getIncludedVariableNames() {
    unordered_set<string> result;
    for (auto& variableExpression : getIncludedVariableExpressions()) {
        result.insert(variableExpression->getInternalName());
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getIncludedVariableExpressions() {
    return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{VARIABLE});
}

vector<shared_ptr<Expression>> Expression::getIncludedPropertyExpressions() {
    return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{PROPERTY});
}

vector<shared_ptr<Expression>> Expression::getIncludedLeafExpressions() {
    return getIncludedExpressionsWithTypes(
        unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
}

vector<shared_ptr<Expression>> Expression::getIncludedSubqueryExpressions() {
    return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{EXISTENTIAL_SUBQUERY});
}

unique_ptr<Expression> Expression::copy() {
    if (children.size() == 2) {
        return make_unique<Expression>(expressionType, dataType, children[0]->copy(), children[1]->copy());
    } else if (children.size() == 1) {
        return make_unique<Expression>(expressionType, dataType, children[0]->copy());
    } else {
        assert(children.empty());
        return make_unique<Expression>(expressionType, dataType);
    }
}

vector<shared_ptr<Expression>> Expression::getIncludedExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) {
    auto result = vector<shared_ptr<Expression>>();
    if (expressionTypes.contains(expressionType)) {
        result.push_back(shared_from_this());
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
