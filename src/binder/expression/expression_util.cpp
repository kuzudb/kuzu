#include "binder/expression/expression_util.h"

#include "common/exception/binder.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

bool ExpressionUtil::isExpressionsWithDataType(const expression_vector& expressions,
    common::LogicalTypeID dataTypeID) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != dataTypeID) {
            return false;
        }
    }
    return true;
}

expression_vector ExpressionUtil::getExpressionsWithDataType(const expression_vector& expressions,
    common::LogicalTypeID dataTypeID) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() == dataTypeID) {
            result.push_back(expression);
        }
    }
    return result;
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

std::string ExpressionUtil::toString(const std::vector<expression_pair>& expressionPairs) {
    if (expressionPairs.empty()) {
        return std::string{};
    }
    auto result = toString(expressionPairs[0]);
    for (auto i = 1u; i < expressionPairs.size(); ++i) {
        result += "," + toString(expressionPairs[i]);
    }
    return result;
}

std::string ExpressionUtil::toString(const expression_pair& expressionPair) {
    return expressionPair.first->toString() + "=" + expressionPair.second->toString();
}

expression_vector ExpressionUtil::excludeExpression(const expression_vector& exprs,
    const Expression& exprToExclude) {
    expression_vector result;
    for (auto& expr : exprs) {
        if (*expr != exprToExclude) {
            result.push_back(expr);
        }
    }
    return result;
}

expression_vector ExpressionUtil::excludeExpressions(const expression_vector& expressions,
    const expression_vector& expressionsToExclude) {
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

logical_type_vec_t ExpressionUtil::getDataTypes(const expression_vector& expressions) {
    std::vector<LogicalType> result;
    result.reserve(expressions.size());
    for (auto& expression : expressions) {
        result.push_back(*expression->getDataType().copy());
    }
    return result;
}

expression_vector ExpressionUtil::removeDuplication(const expression_vector& expressions) {
    expression_vector result;
    expression_set expressionSet;
    for (auto& expression : expressions) {
        if (expressionSet.contains(expression)) {
            continue;
        }
        result.push_back(expression);
        expressionSet.insert(expression);
    }
    return result;
}

bool ExpressionUtil::isNodePattern(const Expression& expression) {
    return expression.expressionType == ExpressionType::PATTERN &&
           expression.dataType.getLogicalTypeID() == LogicalTypeID::NODE;
};

bool ExpressionUtil::isRelPattern(const Expression& expression) {
    return expression.expressionType == ExpressionType::PATTERN &&
           expression.dataType.getLogicalTypeID() == LogicalTypeID::REL;
}

bool ExpressionUtil::isRecursiveRelPattern(const Expression& expression) {
    return expression.expressionType == ExpressionType::PATTERN &&
           expression.dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL;
}

void ExpressionUtil::validateExpressionType(const Expression& expr,
    common::ExpressionType expectedType) {
    if (expr.expressionType == expectedType) {
        return;
    }
    throw BinderException(stringFormat("{} has type {} but {} was expected.", expr.toString(),
        expressionTypeToString(expr.expressionType), expressionTypeToString(expectedType)));
}

void ExpressionUtil::validateDataType(const Expression& expr, const LogicalType& expectedType) {
    if (expr.getDataType() == expectedType) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), expectedType.toString()));
}

void ExpressionUtil::validateDataType(const Expression& expr, LogicalTypeID expectedTypeID) {
    if (expr.getDataType().getLogicalTypeID() == expectedTypeID) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), LogicalTypeUtils::toString(expectedTypeID)));
}

void ExpressionUtil::validateDataType(const Expression& expr,
    const std::vector<LogicalTypeID>& expectedTypeIDs) {
    auto targetsSet =
        std::unordered_set<LogicalTypeID>{expectedTypeIDs.begin(), expectedTypeIDs.end()};
    if (targetsSet.contains(expr.getDataType().getLogicalTypeID())) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), LogicalTypeUtils::toString(expectedTypeIDs)));
}

bool ExpressionUtil::tryCombineDataType(const expression_vector& expressions, LogicalType& result) {
    std::vector<common::LogicalType> inputTypes;
    for (auto& expr : expressions) {
        inputTypes.push_back(expr->getDataType());
    }
    return LogicalTypeUtils::tryGetMaxLogicalType(inputTypes, result);
}

} // namespace binder
} // namespace kuzu
