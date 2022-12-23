#include "processor/mapper/expression_mapper.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "expression_evaluator/case_evaluator.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"

namespace kuzu {
namespace processor {

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapExpression(
    const shared_ptr<Expression>& expression, const Schema& schema) {
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

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapLiteralExpression(
    const shared_ptr<Expression>& expression) {
    auto& literalExpression = (LiteralExpression&)*expression;
    return make_unique<LiteralExpressionEvaluator>(
        make_shared<Literal>(*literalExpression.literal));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapParameterExpression(
    const shared_ptr<Expression>& expression) {
    auto& parameterExpression = (ParameterExpression&)*expression;
    assert(parameterExpression.getLiteral() != nullptr);
    return make_unique<LiteralExpressionEvaluator>(parameterExpression.getLiteral());
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapReferenceExpression(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    auto vectorPos = DataPos(schema.getExpressionPos(*expression));
    auto expressionGroup = schema.getGroup(expression->getUniqueName());
    return make_unique<ReferenceExpressionEvaluator>(vectorPos, expressionGroup->isFlat());
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapCaseExpression(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    auto& caseExpression = (CaseExpression&)*expression;
    vector<unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    for (auto i = 0u; i < caseExpression.getNumCaseAlternatives(); ++i) {
        auto alternative = caseExpression.getCaseAlternative(i);
        auto whenEvaluator = mapExpression(alternative->whenExpression, schema);
        auto thenEvaluator = mapExpression(alternative->thenExpression, schema);
        alternativeEvaluators.push_back(make_unique<CaseAlternativeEvaluator>(
            std::move(whenEvaluator), std::move(thenEvaluator)));
    }
    auto elseEvaluator = mapExpression(caseExpression.getElseExpression(), schema);
    return make_unique<CaseExpressionEvaluator>(
        expression, std::move(alternativeEvaluators), std::move(elseEvaluator));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapFunctionExpression(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    vector<unique_ptr<BaseExpressionEvaluator>> children;
    for (auto i = 0u; i < expression->getNumChildren(); ++i) {
        children.push_back(mapExpression(expression->getChild(i), schema));
    }
    return make_unique<FunctionExpressionEvaluator>(expression, std::move(children));
}

} // namespace processor
} // namespace kuzu
