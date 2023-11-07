#include "binder/expression/expression_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

bool ExpressionUtil::isExpressionsWithDataType(
    const expression_vector& expressions, common::LogicalTypeID dataTypeID) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != dataTypeID) {
            return false;
        }
    }
    return true;
}

expression_vector ExpressionUtil::getExpressionsWithDataType(
    const expression_vector& expressions, common::LogicalTypeID dataTypeID) {
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

std::vector<std::unique_ptr<common::LogicalType>> ExpressionUtil::getDataTypes(
    const kuzu::binder::expression_vector& expressions) {
    std::vector<std::unique_ptr<common::LogicalType>> result;
    result.reserve(expressions.size());
    for (auto& expression : expressions) {
        result.push_back(expression->getDataType().copy());
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

bool ExpressionUtil::isRecursiveRelPattern(const kuzu::binder::Expression& expression) {
    return expression.expressionType == ExpressionType::PATTERN &&
           expression.dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL;
}

} // namespace binder
} // namespace kuzu
