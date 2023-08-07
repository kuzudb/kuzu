#include "processor/expression_mapper.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression/path_expression.h"
#include "binder/expression/rel_expression.h"
#include "expression_evaluator/case_evaluator.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/node_rel_evaluator.h"
#include "expression_evaluator/path_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "planner/logical_plan/schema.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::evaluator;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapExpression(
    const std::shared_ptr<Expression>& expression, const Schema& schema) {
    auto expressionType = expression->expressionType;
    if (schema.isExpressionInScope(*expression)) {
        return mapReferenceExpression(expression, schema);
    } else if (isExpressionLiteral(expressionType)) {
        return mapLiteralExpression(expression);
    } else if (ExpressionUtil::isNodeVariable(*expression)) {
        return mapNodeExpression(expression, schema);
    } else if (ExpressionUtil::isRelVariable(*expression)) {
        return mapRelExpression(expression, schema);
    } else if (expressionType == ExpressionType::PATH) {
        return mapPathExpression(expression, schema);
    } else if (expressionType == ExpressionType::PARAMETER) {
        return mapParameterExpression(expression);
    } else if (CASE_ELSE == expressionType) {
        return mapCaseExpression(expression, schema);
    } else {
        return mapFunctionExpression(expression, schema);
    }
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapLiteralExpression(
    const std::shared_ptr<Expression>& expression) {
    auto& literalExpression = (LiteralExpression&)*expression;
    return std::make_unique<LiteralExpressionEvaluator>(
        std::make_shared<Value>(*literalExpression.getValue()));
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapParameterExpression(
    const std::shared_ptr<binder::Expression>& expression) {
    auto& parameterExpression = (ParameterExpression&)*expression;
    assert(parameterExpression.getLiteral() != nullptr);
    return std::make_unique<LiteralExpressionEvaluator>(parameterExpression.getLiteral());
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapReferenceExpression(
    const std::shared_ptr<binder::Expression>& expression, const Schema& schema) {
    auto vectorPos = DataPos(schema.getExpressionPos(*expression));
    auto expressionGroup = schema.getGroup(expression->getUniqueName());
    return std::make_unique<ReferenceExpressionEvaluator>(vectorPos, expressionGroup->isFlat());
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapCaseExpression(
    const std::shared_ptr<binder::Expression>& expression, const Schema& schema) {
    auto& caseExpression = (CaseExpression&)*expression;
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    for (auto i = 0u; i < caseExpression.getNumCaseAlternatives(); ++i) {
        auto alternative = caseExpression.getCaseAlternative(i);
        auto whenEvaluator = mapExpression(alternative->whenExpression, schema);
        auto thenEvaluator = mapExpression(alternative->thenExpression, schema);
        alternativeEvaluators.push_back(std::make_unique<CaseAlternativeEvaluator>(
            std::move(whenEvaluator), std::move(thenEvaluator)));
    }
    auto elseEvaluator = mapExpression(caseExpression.getElseExpression(), schema);
    return std::make_unique<CaseExpressionEvaluator>(
        expression, std::move(alternativeEvaluators), std::move(elseEvaluator));
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapFunctionExpression(
    const std::shared_ptr<binder::Expression>& expression, const Schema& schema) {
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> children;
    for (auto i = 0u; i < expression->getNumChildren(); ++i) {
        children.push_back(mapExpression(expression->getChild(i), schema));
    }
    return std::make_unique<FunctionExpressionEvaluator>(expression, std::move(children));
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapNodeExpression(
    const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema) {
    auto node = (NodeExpression*)expression.get();
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> children;
    children.push_back(mapExpression(node->getInternalIDProperty(), schema));
    children.push_back(mapExpression(node->getLabelExpression(), schema));
    for (auto& property : node->getPropertyExpressions()) {
        children.push_back(mapExpression(property->copy(), schema));
    }
    return std::make_unique<NodeRelExpressionEvaluator>(expression, std::move(children));
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapRelExpression(
    const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema) {
    auto rel = (RelExpression*)expression.get();
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> children;
    children.push_back(mapExpression(rel->getSrcNode()->getInternalIDProperty(), schema));
    children.push_back(mapExpression(rel->getDstNode()->getInternalIDProperty(), schema));
    children.push_back(mapExpression(rel->getLabelExpression(), schema));
    for (auto& property : rel->getPropertyExpressions()) {
        children.push_back(mapExpression(property->copy(), schema));
    }
    return std::make_unique<NodeRelExpressionEvaluator>(expression, std::move(children));
}

std::unique_ptr<evaluator::ExpressionEvaluator> ExpressionMapper::mapPathExpression(
    const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema) {
    auto pathExpression = std::static_pointer_cast<binder::PathExpression>(expression);
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> children;
    children.reserve(pathExpression->getNumChildren());
    for (auto i = 0u; i < pathExpression->getNumChildren(); ++i) {
        children.push_back(mapExpression(pathExpression->getChild(i), schema));
    }
    return std::make_unique<PathExpressionEvaluator>(pathExpression, std::move(children));
}

} // namespace processor
} // namespace kuzu
