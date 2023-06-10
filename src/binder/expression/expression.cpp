#include "binder/expression/expression.h"

#include "binder/expression/property_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unordered_set<std::string> Expression::getDependentVariableNames() {
    std::unordered_set<std::string> result;
    if (expressionType == VARIABLE) {
        result.insert(getUniqueName());
        return result;
    }
    if (expressionType == common::PROPERTY) {
        result.insert(((PropertyExpression*)this)->getVariableName());
    }
    for (auto& child : getChildren()) {
        for (auto& variableName : child->getDependentVariableNames()) {
            result.insert(variableName);
        }
    }
    return result;
}

expression_vector Expression::getSubPropertyExpressions() {
    expression_vector result;
    if (expressionType == PROPERTY) {
        result.push_back(shared_from_this());
    }
    for (auto& child : getChildren()) { // NOTE: use getChildren interface because we need the
                                        // property from nested subqueries too
        for (auto& expr : child->getSubPropertyExpressions()) {
            result.push_back(expr);
        }
    }
    return result;
}

expression_vector Expression::getTopLevelSubSubqueryExpressions() {
    expression_vector result;
    if (expressionType == EXISTENTIAL_SUBQUERY) {
        result.push_back(shared_from_this());
        return result;
    }
    for (auto& child : children) {
        for (auto& expression : child->getTopLevelSubSubqueryExpressions()) {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector Expression::splitOnAND() {
    expression_vector result;
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

bool ExpressionUtil::allExpressionsHaveDataType(
    expression_vector& expressions, LogicalTypeID dataTypeID) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != dataTypeID) {
            return false;
        }
    }
    return true;
}

uint32_t ExpressionUtil::find(Expression* target, expression_vector expressions) {
    for (auto i = 0u; i < expressions.size(); ++i) {
        if (target->getUniqueName() == expressions[i]->getUniqueName()) {
            return i;
        }
    }
    return UINT32_MAX;
}

std::string ExpressionUtil::toString(const expression_vector& expressions) {
    if (expressions.empty()) {
        return std::string{};
    }
    auto result = expressions[0]->toString();
    for (auto i = 1u; i < expressions.size(); ++i) {
        result += "," + expressions[i]->toString();
    }
    return result;
}

expression_vector ExpressionUtil::excludeExpressions(
    const expression_vector& expressions, const expression_vector& expressionsToExclude) {
    expression_set excludeSet;
    for (auto& expression : expressionsToExclude) {
        excludeSet.insert(expression);
    }
    expression_vector result;
    for (auto& expression : expressions) {
        if (!excludeSet.contains(expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
