#include "binder/expression_visitor.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression/subquery_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector ExpressionChildrenCollector::collectChildren(const Expression& expression) {
    switch (expression.expressionType) {
    case ExpressionType::CASE_ELSE: {
        return collectCaseChildren(expression);
    }
    case ExpressionType::SUBQUERY: {
        return collectSubqueryChildren(expression);
    }
    case ExpressionType::PATTERN: {
        switch (expression.dataType.getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            return collectNodeChildren(expression);
        }
        case LogicalTypeID::REL: {
            return collectRelChildren(expression);
        }
        default: {
            return expression_vector{};
        }
        }
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

expression_vector ExpressionChildrenCollector::collectSubqueryChildren(
    const Expression& expression) {
    expression_vector result;
    auto& subqueryExpression = (SubqueryExpression&)expression;
    for (auto& node : subqueryExpression.getQueryGraphCollection()->getQueryNodes()) {
        result.push_back(node->getInternalID());
    }
    if (subqueryExpression.hasWhereExpression()) {
        result.push_back(subqueryExpression.getWhereExpression());
    }
    return result;
}

expression_vector ExpressionChildrenCollector::collectNodeChildren(const Expression& expression) {
    expression_vector result;
    auto& node = (NodeExpression&)expression;
    for (auto& property : node.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    result.push_back(node.getInternalID());
    return result;
}

expression_vector ExpressionChildrenCollector::collectRelChildren(const Expression& expression) {
    expression_vector result;
    auto& rel = (RelExpression&)expression;
    result.push_back(rel.getSrcNode()->getInternalID());
    result.push_back(rel.getDstNode()->getInternalID());
    for (auto& property : rel.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    return result;
}

// isConstant requires all children to be constant.
bool ExpressionVisitor::isConstant(const Expression& expression) {
    if (expression.expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        return false; // We don't have a framework to fold aggregated constant.
    }
    if (expression.getNumChildren() == 0) {
        return expression.expressionType == ExpressionType::LITERAL;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(expression)) {
        if (!isConstant(*child)) {
            return false;
        }
    }
    return true;
}

bool ExpressionVisitor::satisfyAny(
    const Expression& expression, const std::function<bool(const Expression&)>& condition) {
    if (condition(expression)) {
        return true;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(expression)) {
        if (satisfyAny(*child, condition)) {
            return true;
        }
    }
    return false;
}

std::unordered_set<std::string> ExpressionCollector::getDependentVariableNames(
    const std::shared_ptr<Expression>& expression) {
    KU_ASSERT(expressions.empty());
    collectExpressionsInternal(expression, [&](const Expression& expression) {
        return expression.expressionType == ExpressionType::PROPERTY ||
               expression.expressionType == ExpressionType::PATTERN ||
               expression.expressionType == ExpressionType::VARIABLE;
    });
    std::unordered_set<std::string> result;
    for (auto& expr : expressions) {
        if (expr->expressionType == ExpressionType::PROPERTY) {
            auto property = (PropertyExpression*)expr.get();
            result.insert(property->getVariableName());
        } else {
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
