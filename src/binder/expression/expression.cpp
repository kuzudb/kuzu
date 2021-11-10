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

bool Expression::hasAggregationExpression() const {
    if (isExpressionAggregate(expressionType)) {
        return true;
    }
    for (auto& child : children) {
        if (child->hasAggregationExpression()) {
            return true;
        }
    }
    return false;
}

bool Expression::hasSubqueryExpression() const {
    if (expressionType == EXISTENTIAL_SUBQUERY) {
        return true;
    }
    for (auto& child : children) {
        if (child->hasSubqueryExpression()) {
            return true;
        }
    }
    return false;
}

unordered_set<string> Expression::getDependentVariableNames() {
    unordered_set<string> result;
    for (auto& variableExpression : getDependentVariables()) {
        result.insert(variableExpression->getUniqueName());
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getDependentVariables() {
    if (expressionType == VARIABLE) {
        return vector<shared_ptr<Expression>>{shared_from_this()};
    }
    vector<shared_ptr<Expression>> result;
    for (auto& child : children) {
        for (auto& expression : child->getDependentVariables()) {
            result.push_back(expression);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getDependentProperties() {
    if (expressionType == PROPERTY) {
        return vector<shared_ptr<Expression>>{shared_from_this()};
    }
    vector<shared_ptr<Expression>> result;
    for (auto& child : children) {
        for (auto& expression : child->getDependentProperties()) {
            result.push_back(expression);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getDependentLeafExpressions() {
    if (expressionType == PROPERTY || expressionType == CSV_LINE_EXTRACT) {
        return vector<shared_ptr<Expression>>{shared_from_this()};
    }
    vector<shared_ptr<Expression>> result;
    for (auto& child : children) {
        for (auto& expression : child->getDependentLeafExpressions()) {
            result.push_back(expression);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> Expression::getDependentSubqueryExpressions() {
    if (expressionType == EXISTENTIAL_SUBQUERY) {
        return vector<shared_ptr<Expression>>{shared_from_this()};
    }
    vector<shared_ptr<Expression>> result;
    for (auto& child : children) {
        for (auto& expression : child->getDependentSubqueryExpressions()) {
            result.push_back(expression);
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

} // namespace binder
} // namespace graphflow
