#include "processor/expression_mapper.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression/path_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression_visitor.h"
#include "common/string_utils.h"
#include "expression_evaluator/case_evaluator.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/node_rel_evaluator.h"
#include "expression_evaluator/path_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "planner/operator/schema.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::evaluator;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static bool canEvaluateAsFunction(ExpressionType expressionType) {
    switch (expressionType) {
    case ExpressionType::OR:
    case ExpressionType::XOR:
    case ExpressionType::AND:
    case ExpressionType::NOT:
    case ExpressionType::EQUALS:
    case ExpressionType::NOT_EQUALS:
    case ExpressionType::GREATER_THAN:
    case ExpressionType::GREATER_THAN_EQUALS:
    case ExpressionType::LESS_THAN:
    case ExpressionType::LESS_THAN_EQUALS:
    case ExpressionType::IS_NULL:
    case ExpressionType::IS_NOT_NULL:
    case ExpressionType::FUNCTION:
        return true;
    default:
        return false;
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getEvaluator(
    const std::shared_ptr<Expression>& expression, const Schema* schema) {
    if (schema == nullptr) {
        return getConstantEvaluator(expression);
    }
    auto expressionType = expression->expressionType;
    if (schema->isExpressionInScope(*expression)) {
        return getReferenceEvaluator(expression, schema);
    } else if (isExpressionLiteral(expressionType)) {
        return getLiteralEvaluator(*expression);
    } else if (ExpressionUtil::isNodeVariable(*expression)) {
        return getNodeEvaluator(expression, schema);
    } else if (ExpressionUtil::isRelVariable(*expression)) {
        return getRelEvaluator(expression, schema);
    } else if (expressionType == ExpressionType::PATH) {
        return getPathEvaluator(expression, schema);
    } else if (expressionType == ExpressionType::PARAMETER) {
        return getParameterEvaluator(*expression);
    } else if (CASE_ELSE == expressionType) {
        return getCaseEvaluator(expression, schema);
    } else if (canEvaluateAsFunction(expressionType)) {
        return getFunctionEvaluator(expression, schema);
    } else {
        throw NotImplementedException(StringUtils::string_format(
            "Cannot evaluate expression with type {}.", expressionTypeToString(expressionType)));
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getConstantEvaluator(
    const std::shared_ptr<Expression>& expression) {
    assert(ExpressionVisitor::isConstant(*expression));
    auto expressionType = expression->expressionType;
    if (isExpressionLiteral(expressionType)) {
        return getLiteralEvaluator(*expression);
    } else if (CASE_ELSE == expressionType) {
        return getCaseEvaluator(expression, nullptr);
    } else if (canEvaluateAsFunction(expressionType)) {
        return getFunctionEvaluator(expression, nullptr);
    } else {
        throw NotImplementedException(StringUtils::string_format(
            "Cannot evaluate expression with type {}.", expressionTypeToString(expressionType)));
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getLiteralEvaluator(
    const Expression& expression) {
    auto& literalExpression = (LiteralExpression&)expression;
    return std::make_unique<LiteralExpressionEvaluator>(
        std::make_shared<Value>(*literalExpression.getValue()));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getParameterEvaluator(
    const Expression& expression) {
    auto& parameterExpression = (ParameterExpression&)expression;
    assert(parameterExpression.getLiteral() != nullptr);
    return std::make_unique<LiteralExpressionEvaluator>(parameterExpression.getLiteral());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getReferenceEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    assert(schema != nullptr);
    auto vectorPos = DataPos(schema->getExpressionPos(*expression));
    auto expressionGroup = schema->getGroup(expression->getUniqueName());
    return std::make_unique<ReferenceExpressionEvaluator>(vectorPos, expressionGroup->isFlat());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getCaseEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto caseExpression = reinterpret_cast<CaseExpression*>(expression.get());
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    for (auto i = 0u; i < caseExpression->getNumCaseAlternatives(); ++i) {
        auto alternative = caseExpression->getCaseAlternative(i);
        auto whenEvaluator = getEvaluator(alternative->whenExpression, schema);
        auto thenEvaluator = getEvaluator(alternative->thenExpression, schema);
        alternativeEvaluators.push_back(std::make_unique<CaseAlternativeEvaluator>(
            std::move(whenEvaluator), std::move(thenEvaluator)));
    }
    auto elseEvaluator = getEvaluator(caseExpression->getElseExpression(), schema);
    return std::make_unique<CaseExpressionEvaluator>(
        std::move(expression), std::move(alternativeEvaluators), std::move(elseEvaluator));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getFunctionEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto childrenEvaluators = getEvaluators(expression->getChildren(), schema);
    return std::make_unique<FunctionExpressionEvaluator>(
        std::move(expression), std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getNodeEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto node = (NodeExpression*)expression.get();
    expression_vector children;
    children.push_back(node->getInternalIDProperty());
    children.push_back(node->getLabelExpression());
    for (auto& property : node->getPropertyExpressions()) {
        children.push_back(property->copy());
    }
    auto childrenEvaluators = getEvaluators(children, schema);
    return std::make_unique<NodeRelExpressionEvaluator>(
        std::move(expression), std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getRelEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto rel = (RelExpression*)expression.get();
    expression_vector children;
    children.push_back(rel->getSrcNode()->getInternalIDProperty());
    children.push_back(rel->getDstNode()->getInternalIDProperty());
    children.push_back(rel->getLabelExpression());
    for (auto& property : rel->getPropertyExpressions()) {
        children.push_back(property->copy());
    }
    auto childrenEvaluators = getEvaluators(children, schema);
    return std::make_unique<NodeRelExpressionEvaluator>(
        std::move(expression), std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getPathEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto childrenEvaluators = getEvaluators(expression->getChildren(), schema);
    return std::make_unique<PathExpressionEvaluator>(
        std::move(expression), std::move(childrenEvaluators));
}

std::vector<std::unique_ptr<ExpressionEvaluator>> ExpressionMapper::getEvaluators(
    const binder::expression_vector& expressions, const Schema* schema) {
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    evaluators.reserve(expressions.size());
    for (auto& expression : expressions) {
        evaluators.push_back(getEvaluator(expression, schema));
    }
    return evaluators;
}

} // namespace processor
} // namespace kuzu
