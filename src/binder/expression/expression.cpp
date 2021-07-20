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
    auto result = unordered_set<string>();
    if (isExpressionLiteral(expressionType)) {
        return result;
    } else if (VARIABLE == expressionType) {
        result.insert({getInternalName()});
    } else {
        for (auto& child : children) {
            auto tmp = child->getIncludedVariableNames();
            result.insert(begin(tmp), end(tmp));
        }
    }
    return result;
}

vector<const Expression*> Expression::getIncludedExpressions(
    const unordered_set<ExpressionType>& expressionTypes) const {
    auto result = vector<const Expression*>();
    if (expressionTypes.contains(expressionType)) {
        result.push_back(this);
        return result;
    }
    for (auto& child : children) {
        auto tmp = child->getIncludedExpressions(expressionTypes);
        result.insert(end(result), begin(tmp), end(tmp));
    }
    return result;
}

} // namespace binder
} // namespace graphflow
