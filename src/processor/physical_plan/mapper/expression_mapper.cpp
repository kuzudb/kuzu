#include "src/processor/include/physical_plan/mapper/expression_mapper.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/expression_evaluator/include/unary_expression_evaluator.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalExpressionToPhysical(
    const Expression& expression, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto expressionType = expression.expressionType;
    unique_ptr<ExpressionEvaluator> retVal;
    if (isExpressionLiteral(expressionType)) {
        retVal = mapLogicalLiteralExpressionToStructuredPhysical(expression, executionContext);
    } else if (mapperContext.expressionHasComputed(expression.getUniqueName())) {
        /**
         * A leaf expression is a non-literal expression that has been previously computed
         */
        retVal = mapLogicalLeafExpressionToPhysical(expression, mapperContext);
    } else if (isExpressionAggregate(expressionType)) {
        if (expressionType == COUNT_STAR_FUNC) {
            // COUNT_STAR has no child expression
            assert(expression.getChildren().empty());
            retVal = make_unique<AggregateExpressionEvaluator>(expressionType, expression.dataType,
                AggregateExpressionEvaluator::getAggregationFunction(
                    expressionType, expression.dataType));
        } else {
            auto child = mapLogicalExpressionToPhysical(
                *expression.getChild(0), mapperContext, executionContext);
            retVal = make_unique<AggregateExpressionEvaluator>(expressionType, expression.dataType,
                move(child),
                AggregateExpressionEvaluator::getAggregationFunction(
                    expressionType, child->dataType));
        }
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapLogicalExpressionToPhysical(
            *expression.getChild(0), mapperContext, executionContext);
        retVal =
            make_unique<UnaryExpressionEvaluator>(move(child), expressionType, expression.dataType);
    } else {
        assert(isExpressionBinary(expressionType));
        // If one of the children expressions has UNSTRUCTURED data type, i.e. the type of the data
        // is not fixed and can take on multiple values, then we add a cast operation to the other
        // child (even if the other child's type is known in advance and is structured). The
        // UNSTRUCTURED child's data will be stored in vectors that store boxed values instead of
        // structured (i.e., primitive) values. The cast operation ensures that the other child's
        // values are also boxed so we can call expression evaluation functions that expect two
        // boxed values.
        auto& logicalLExpr = *expression.getChild(0);
        auto& logicalRExpr = *expression.getChild(1);
        auto castLToUnstructured =
            logicalRExpr.dataType == UNSTRUCTURED && logicalLExpr.dataType != UNSTRUCTURED;
        auto lExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            logicalLExpr, castLToUnstructured, mapperContext, executionContext);
        auto castRToUnstructured =
            logicalLExpr.dataType == UNSTRUCTURED && logicalRExpr.dataType != UNSTRUCTURED;
        auto rExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            logicalRExpr, castRToUnstructured, mapperContext, executionContext);
        retVal = make_unique<BinaryExpressionEvaluator>(
            move(lExpr), move(rExpr), expressionType, expression.dataType);
    }
    return retVal;
}

unique_ptr<ExpressionEvaluator>
ExpressionMapper::mapChildExpressionAndCastToUnstructuredIfNecessary(const Expression& expression,
    bool castToUnstructured, const MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    unique_ptr<ExpressionEvaluator> retVal;
    if (castToUnstructured && isExpressionLiteral(expression.expressionType)) {
        retVal = mapLogicalLiteralExpressionToUnstructuredPhysical(expression, executionContext);
    } else {
        retVal = mapLogicalExpressionToPhysical(expression, mapperContext, executionContext);
        if (castToUnstructured) {
            retVal = make_unique<UnaryExpressionEvaluator>(
                move(retVal), CAST_TO_UNSTRUCTURED_VALUE, UNSTRUCTURED);
        }
    }
    return retVal;
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalLiteralExpressionToUnstructuredPhysical(
    const Expression& expression, ExecutionContext& executionContext) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        executionContext.memoryManager, UNSTRUCTURED, true /* isSingleValue */);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    auto& val = ((Value*)vector->values)[0];
    val.dataType = literalExpression.literal.dataType;
    switch (val.dataType) {
    case INT64: {
        val.val.int64Val = literalExpression.literal.val.int64Val;
    } break;
    case DOUBLE: {
        val.val.doubleVal = literalExpression.literal.val.doubleVal;
    } break;
    case BOOL: {
        val.val.booleanVal = literalExpression.literal.val.booleanVal;
    } break;
    case DATE: {
        val.val.dateVal = literalExpression.literal.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.val.timestampVal = literalExpression.literal.val.timestampVal;
    } break;
    case INTERVAL: {
        val.val.intervalVal = literalExpression.literal.val.intervalVal;
    } break;
    case STRING: {
        vector->allocateStringOverflowSpace(
            val.val.strVal, literalExpression.literal.strVal.length());
        val.val.strVal.set(literalExpression.literal.strVal);
    } break;
    default:
        assert(false);
    }
    return make_unique<ExpressionEvaluator>(vector, expression.expressionType);
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalLiteralExpressionToStructuredPhysical(
    const Expression& expression, ExecutionContext& executionContext) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        executionContext.memoryManager, literalExpression.dataType, true /* isSingleValue */);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    switch (expression.dataType) {
    case INT64: {
        ((int64_t*)vector->values)[0] = literalExpression.literal.val.int64Val;
    } break;
    case DOUBLE: {
        ((double_t*)vector->values)[0] = literalExpression.literal.val.doubleVal;
    } break;
    case BOOL: {
        ((bool*)vector->values)[0] = literalExpression.literal.val.booleanVal;
    } break;
    case STRING: {
        vector->addString(0 /* pos */, literalExpression.literal.strVal);
    } break;
    case DATE: {
        ((date_t*)vector->values)[0] = literalExpression.literal.val.dateVal;
    } break;
    case TIMESTAMP: {
        ((timestamp_t*)vector->values)[0] = literalExpression.literal.val.timestampVal;
    } break;
    case INTERVAL: {
        ((interval_t*)vector->values)[0] = literalExpression.literal.val.intervalVal;
    } break;
    default:
        assert(false);
    }
    return make_unique<ExpressionEvaluator>(vector, expression.expressionType);
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalLeafExpressionToPhysical(
    const Expression& expression, const MapperContext& mapperContext) {
    return make_unique<ExpressionEvaluator>(mapperContext.getDataPos(expression.getUniqueName()),
        expression.expressionType, expression.dataType);
}

} // namespace processor
} // namespace graphflow
