#include "include/expression.h"

namespace graphflow {
namespace binder {

Expression::Expression(ExpressionType expressionType, DataType dataType, expression_vector children)
    : Expression{expressionType, move(dataType)} {
    uniqueName = expressionTypeToString(expressionType) + "(";
    for (auto& child : children) {
        uniqueName += child->getUniqueName() + ", ";
        this->children.push_back(move(child));
    }
    uniqueName += ")";
}

Expression::Expression(ExpressionType expressionType, DataType dataType, const string& uniqueName)
    : Expression{expressionType, move(dataType)} {
    this->uniqueName = uniqueName;
}

Expression::Expression(
    ExpressionType expressionType, DataTypeID dataTypeID, const string& uniqueName)
    : Expression{expressionType, DataType(dataTypeID), uniqueName} {
    assert(dataTypeID != LIST);
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

} // namespace binder
} // namespace graphflow
