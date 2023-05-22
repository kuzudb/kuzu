#include "processor/mapper/expression_mapper.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "expression_evaluator/case_evaluator.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "planner/logical_plan/logical_operator/schema.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::evaluator;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapExpression(
    const std::shared_ptr<Expression>& expression, const Schema& schema) {
    auto expressionType = expression->expressionType;
    if (schema.isExpressionInScope(*expression)) {
        return mapReferenceExpression(expression, schema);
    } else if (isExpressionLiteral(expressionType)) {
        return mapLiteralExpression(expression);
    } else if (PARAMETER == expressionType) {
        return mapParameterExpression(expression);
    } else if (CASE_ELSE == expressionType) {
        return mapCaseExpression(expression, schema);
    } else {
        return mapFunctionExpression(expression, schema);
    }
}

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapLiteralExpression(
    const std::shared_ptr<Expression>& expression) {
    auto& literalExpression = (LiteralExpression&)*expression;
    return std::make_unique<LiteralExpressionEvaluator>(
        std::make_shared<Value>(*literalExpression.getValue()));
}

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapParameterExpression(
    const std::shared_ptr<binder::Expression>& expression) {
    auto& parameterExpression = (ParameterExpression&)*expression;
    assert(parameterExpression.getLiteral() != nullptr);
    return std::make_unique<LiteralExpressionEvaluator>(parameterExpression.getLiteral());
}

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapReferenceExpression(
    const std::shared_ptr<binder::Expression>& expression, const Schema& schema) {
    auto vectorPos = DataPos(schema.getExpressionPos(*expression));
    auto expressionGroup = schema.getGroup(expression->getUniqueName());
    return std::make_unique<ReferenceExpressionEvaluator>(vectorPos, expressionGroup->isFlat());
}

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapCaseExpression(
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

std::unique_ptr<evaluator::BaseExpressionEvaluator> ExpressionMapper::mapFunctionExpression(
    const std::shared_ptr<binder::Expression>& expression, const Schema& schema) {
    std::vector<std::unique_ptr<evaluator::BaseExpressionEvaluator>> children;
    for (auto i = 0u; i < expression->getNumChildren(); ++i) {
        children.push_back(mapExpression(expression->getChild(i), schema));
    }
    return std::make_unique<FunctionExpressionEvaluator>(expression, std::move(children));
}

} // namespace processor
} // namespace kuzu
