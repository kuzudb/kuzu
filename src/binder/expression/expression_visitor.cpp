#include "binder/expression/expression_visitor.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/existential_subquery_expression.h"
#include "binder/expression/property_expression.h"

namespace kuzu {
namespace binder {

expression_vector ExpressionChildrenCollector::collectChildren(const Expression& expression) {
    switch (expression.expressionType) {
    case common::ExpressionType::CASE_ELSE: {
        return collectCaseChildren(expression);
    }
    case common::ExpressionType::EXISTENTIAL_SUBQUERY: {
        return collectExistentialSubqueryChildren(expression);
    }
    default: {
        return expression.children;
    }
    }
}

expression_vector ExpressionChildrenCollector::collectCaseChildren(const Expression& expression) {
    expression_vector result;
    auto& caseExpression = (CaseExpression&)expression;
    for (auto i = 0u; i < caseExpression.getNumCaseAlternatives(); ++i) {
        auto caseAlternative = caseExpression.getCaseAlternative(i);
        result.push_back(caseAlternative->whenExpression);
        result.push_back(caseAlternative->thenExpression);
    }
    result.push_back(caseExpression.getElseExpression());
    return result;
}

expression_vector ExpressionChildrenCollector::collectExistentialSubqueryChildren(
    const Expression& expression) {
    expression_vector result;
    auto& subqueryExpression = (ExistentialSubqueryExpression&)expression;
    for (auto& node : subqueryExpression.getQueryGraphCollection()->getQueryNodes()) {
        result.push_back(node->getInternalIDProperty());
    }
    if (subqueryExpression.hasWhereExpression()) {
        result.push_back(subqueryExpression.getWhereExpression());
    }
    return result;
}

bool ExpressionVisitor::hasExpression(
    const Expression& expression, const std::function<bool(const Expression&)>& condition) {
    if (condition(expression)) {
        return true;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(expression)) {
        if (hasExpression(*child, condition)) {
            return true;
        }
    }
    return false;
}

std::unordered_set<std::string> ExpressionCollector::getDependentVariableNames(
    const std::shared_ptr<Expression>& expression) {
    assert(expressions.empty());
    collectExpressionsInternal(expression, [&](const Expression& expression) {
        return expression.expressionType == common::ExpressionType::PROPERTY ||
               expression.expressionType == common::ExpressionType::VARIABLE;
    });
    std::unordered_set<std::string> result;
    for (auto& expr : expressions) {
        if (expr->expressionType == common::ExpressionType::PROPERTY) {
            auto property = (PropertyExpression*)expr.get();
            result.insert(property->getVariableName());
        } else {
            assert(expr->expressionType == common::ExpressionType::VARIABLE);
            result.insert(expr->getUniqueName());
        }
    }
    return result;
}

void ExpressionCollector::collectExpressionsInternal(const std::shared_ptr<Expression>& expression,
    const std::function<bool(const Expression&)>& condition) {
    if (condition(*expression)) {
        expressions.push_back(expression);
        return;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
        collectExpressionsInternal(child, condition);
    }
}

} // namespace binder
} // namespace kuzu
