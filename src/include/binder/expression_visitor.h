#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class ExpressionChildrenCollector {
public:
    static expression_vector collectChildren(const Expression& expression);

private:
    static expression_vector collectCaseChildren(const Expression& expression);

    static expression_vector collectSubqueryChildren(const Expression& expression);

    static expression_vector collectNodeChildren(const Expression& expression);

    static expression_vector collectRelChildren(const Expression& expression);
};

class ExpressionVisitor {
public:
    static inline bool hasAggregate(const Expression& expression) {
        return satisfyAny(expression, [&](const Expression& expression) {
            return common::isExpressionAggregate(expression.expressionType);
        });
    }

    static inline bool hasSubquery(const Expression& expression) {
        return satisfyAny(expression, [&](const Expression& expression) {
            return common::isExpressionSubquery(expression.expressionType);
        });
    }

    static inline bool needFold(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::LITERAL ?
                   false :
                   isConstant(expression);
    }

    static bool isConstant(const Expression& expression);

    static bool isRandom(const Expression& expression);

private:
    static bool satisfyAny(const Expression& expression,
        const std::function<bool(const Expression&)>& condition);
};

class ExpressionCollector {
public:
    inline expression_vector collectPropertyExpressions(
        const std::shared_ptr<Expression>& expression) {
        KU_ASSERT(expressions.empty());
        collectExpressionsInternal(expression, [&](const Expression& expression) {
            return expression.expressionType == common::ExpressionType::PROPERTY;
        });
        return expressions;
    }

    inline expression_vector collectTopLevelSubqueryExpressions(
        const std::shared_ptr<Expression>& expression) {
        KU_ASSERT(expressions.empty());
        collectExpressionsInternal(expression, [&](const Expression& expression) {
            return expression.expressionType == common::ExpressionType::SUBQUERY;
        });
        return expressions;
    }

    std::unordered_set<std::string> getDependentVariableNames(
        const std::shared_ptr<Expression>& expression);

private:
    void collectExpressionsInternal(const std::shared_ptr<Expression>& expression,
        const std::function<bool(const Expression&)>& condition);

private:
    expression_vector expressions;
};

} // namespace binder
} // namespace kuzu
