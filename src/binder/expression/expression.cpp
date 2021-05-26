#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

Expression::Expression(ExpressionType expressionType, DataType dataType,
    shared_ptr<Expression> left, shared_ptr<Expression> right)
    : Expression(expressionType, dataType) {
    childrenExpr.push_back(left);
    childrenExpr.push_back(right);
}

Expression::Expression(
    ExpressionType expressionType, DataType dataType, shared_ptr<Expression> child)
    : Expression(expressionType, dataType) {
    childrenExpr.push_back(child);
}

Expression::Expression(ExpressionType expressionType, DataType dataType, const string& variableName)
    : Expression(expressionType, dataType) {
    this->variableName = variableName;
}

unordered_set<string> Expression::getIncludedVariableNames() const {
    auto result = unordered_set<string>();
    if (isExpressionLeafLiteral(expressionType)) {
        return result;
    }
    if (VARIABLE == expressionType) {
        return unordered_set<string>{variableName};
    }
    for (auto& childExpr : childrenExpr) {
        auto tmp = childExpr->getIncludedVariableNames();
        result.insert(begin(tmp), end(tmp));
    }
    return result;
}

vector<const Expression*> Expression::getIncludedExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) const {
    auto result = vector<const Expression*>();
    if (expressionTypes.contains(expressionType)) {
        result.push_back(this);
        return result;
    }
    for (auto& childExpr : childrenExpr) {
        auto tmp = childExpr->getIncludedExpressionsWithTypes(expressionTypes);
        result.insert(end(result), begin(tmp), end(tmp));
    }
    return result;
}

} // namespace binder
} // namespace graphflow
