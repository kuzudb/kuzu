#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

unordered_set<string> Expression::getDependentVariableNames() {
    unordered_set<string> result;
    if (expressionType == VARIABLE) {
        result.insert(getUniqueName());
        return result;
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
    expression_vector& expressions, DataTypeID dataTypeID) {
    for (auto& expression : expressions) {
        if (expression->dataType.typeID != dataTypeID) {
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

string ExpressionUtil::toString(const expression_vector& expressions) {
    if (expressions.empty()) {
        return string{};
    }
    auto result = expressions[0]->getRawName();
    for (auto i = 1u; i < expressions.size(); ++i) {
        result += "," + expressions[i]->getRawName();
    }
    return result;
}

} // namespace binder
} // namespace kuzu
