#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

Expression::Expression(ExpressionType expressionType, DataType dataType,
    const shared_ptr<Expression>& left, const shared_ptr<Expression>& right)
    : Expression{expressionType, dataType} {
    uniqueName = expressionTypeToString(expressionType) + "(" + left->getUniqueName() + "," +
                 right->getUniqueName() + ")";
    children.push_back(left);
    children.push_back(right);
}

Expression::Expression(
    ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& child)
    : Expression{expressionType, dataType} {
    uniqueName = expressionTypeToString(expressionType) + "(" + child->getUniqueName() + ")";
    children.push_back(child);
}

Expression::Expression(ExpressionType expressionType, DataType dataType, const string& name)
    : Expression{expressionType, dataType} {
    uniqueName = name;
}

unordered_set<string> Expression::getDependentVariableNames() {
    unordered_set<string> result;
    if (expressionType == VARIABLE) {
        result.insert(getUniqueName());
        return result;
    }
    for (auto& child : children) {
        for (auto& variableName : child->getDependentVariableNames()) {
            result.insert(variableName);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::splitOnAND() {
    auto result = vector<shared_ptr<Expression>>();
    if (AND == expressionType) {
        for (auto& child : children) {
            for (auto& exp : child->splitOnAND()) {
                result.push_back(exp);
            }
        }
    } else {
        result.push_back(shared_from_this());
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getTopLevelSubExpressionsOfType(
    const std::function<bool(ExpressionType)>& typeCheckFunc) {
    vector<shared_ptr<Expression>> result;
    if (typeCheckFunc(expressionType)) {
        result.push_back(shared_from_this());
        return result;
    }
    for (auto& child : children) {
        for (auto& expression : child->getTopLevelSubExpressionsOfType(typeCheckFunc)) {
            result.push_back(expression);
        }
    }
    return result;
}

bool Expression::hasSubExpressionOfType(
    const std::function<bool(ExpressionType)>& typeCheckFunc) const {
    if (typeCheckFunc(expressionType)) {
        return true;
    }
    for (auto& child : children) {
        if (child->hasSubExpressionOfType(typeCheckFunc)) {
            return true;
        }
    }
    return false;
}

} // namespace binder
} // namespace graphflow
