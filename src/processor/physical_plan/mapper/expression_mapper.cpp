#include "src/processor/include/physical_plan/mapper/expression_mapper.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/expression_evaluator/include/literal_evaluator.h"
#include "src/expression_evaluator/include/operator_evaluator.h"
#include "src/expression_evaluator/include/reference_evaluator.h"

namespace graphflow {
namespace processor {

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto expressionType = expression->expressionType;
    if (isExpressionLiteral(expressionType)) {
        return mapLiteralExpression(expression, false /* castToUnstructured */);
    } else if (mapperContext.expressionHasComputed(expression->getUniqueName())) {
        return mapReferenceExpression(expression, mapperContext);
    } else {
        return mapOperatorExpression(expression, mapperContext, executionContext);
    }
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapLiteralExpression(
    const shared_ptr<Expression>& expression, bool castToUnstructured) {
    auto& literalExpression = (LiteralExpression&)*expression;
    return make_unique<LiteralExpressionEvaluator>(literalExpression.literal, castToUnstructured);
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapReferenceExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext) {
    auto vectorPos = mapperContext.getDataPos(expression->getUniqueName());
    return make_unique<ReferenceExpressionEvaluator>(
        vectorPos, mapperContext.isDataChunkFlat(vectorPos.dataChunkPos));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapOperatorExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    if (isExpressionUnary(expression->expressionType)) {
        return mapUnaryOperatorExpression(expression, mapperContext, executionContext);
    } else {
        assert(isExpressionBinary(expression->expressionType));
        return mapBinaryOperatorExpression(expression, mapperContext, executionContext);
    }
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapUnaryOperatorExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto child = mapExpression(expression->getChild(0), mapperContext, executionContext);
    return make_unique<UnaryOperatorExpressionEvaluator>(expression, move(child));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapBinaryOperatorExpression(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    // NOTE: Xiyang is not convinced that casting should be performed at mapper level.
    auto isLeftUnstructured = expression->getChild(0)->dataType == UNSTRUCTURED;
    auto isRightUnstructured = expression->getChild(1)->dataType == UNSTRUCTURED;
    auto castLeftToUnstructured = !isLeftUnstructured && isRightUnstructured;
    auto castRightToUnstructured = isLeftUnstructured && !isRightUnstructured;
    auto left = castLeftToUnstructured ?
                    mapExpressionAndCastToUnstructured(
                        expression->getChild(0), mapperContext, executionContext) :
                    mapExpression(expression->getChild(0), mapperContext, executionContext);
    auto right = castRightToUnstructured ?
                     mapExpressionAndCastToUnstructured(
                         expression->getChild(1), mapperContext, executionContext) :
                     mapExpression(expression->getChild(1), mapperContext, executionContext);
    return make_unique<BinaryOperatorExpressionEvaluator>(expression, move(left), move(right));
}

unique_ptr<BaseExpressionEvaluator> ExpressionMapper::mapExpressionAndCastToUnstructured(
    const shared_ptr<Expression>& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    if (isExpressionLiteral(expression->expressionType)) {
        return mapLiteralExpression(expression, true /* castToUnstructured */);
    }
    auto castExpression =
        make_shared<Expression>(CAST_TO_UNSTRUCTURED_VALUE, UNSTRUCTURED, expression);
    return mapExpression(castExpression, mapperContext, executionContext);
}

} // namespace processor
} // namespace graphflow
