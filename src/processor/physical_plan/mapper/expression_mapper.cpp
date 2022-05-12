#include "src/processor/include/physical_plan/mapper/expression_mapper.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/binder/expression/include/parameter_expression.h"
#include "src/expression_evaluator/include/function_evaluator.h"
#include "src/expression_evaluator/include/literal_evaluator.h"
#include "src/expression_evaluator/include/reference_evaluator.h"

namespace graphflow {
namespace processor {

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext) {
    auto expressionType = expression->expressionType;
    if (isExpressionLiteral(expressionType)) {
        return mapLiteralExpression(expression);
    } else if (PARAMETER == expressionType) {
        return mapParameterExpression((expression));
    } else if (mapperContext.expressionHasComputed(expression->getUniqueName())) {
        return mapReferenceExpression(expression, mapperContext);
    } else {
        return mapFunctionExpression(expression, mapperContext);
    }
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapLiteralExpression(
    const shared_ptr<Expression>& expression) {
    auto& literalExpression = (LiteralExpression&)*expression;
    return make_unique<LiteralExpressionEvaluator>(make_shared<Literal>(literalExpression.literal));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapParameterExpression(
    const shared_ptr<Expression>& expression) {
    auto& parameterExpression = (ParameterExpression&)*expression;
    assert(parameterExpression.getLiteral() != nullptr);
    return make_unique<LiteralExpressionEvaluator>(parameterExpression.getLiteral());
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapReferenceExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext) {
    auto vectorPos = mapperContext.getDataPos(expression->getUniqueName());
    return make_unique<ReferenceExpressionEvaluator>(
        vectorPos, mapperContext.isDataChunkFlat(vectorPos.dataChunkPos));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapFunctionExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext) {
    vector<unique_ptr<BaseExpressionEvaluator>> children;
    for (auto i = 0u; i < expression->getNumChildren(); ++i) {
        children.push_back(mapExpression(expression->getChild(i), mapperContext));
    }
    return make_unique<FunctionExpressionEvaluator>(expression, move(children));
}

} // namespace processor
} // namespace graphflow
