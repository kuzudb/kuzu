#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/expression_evaluator/include/existential_subquery_evaluator.h"
#include "src/expression_evaluator/include/unary_expression_evaluator.h"
#include "src/processor/include/physical_plan/plan_mapper.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapToPhysical(const Expression& expression,
    const PhysicalOperatorsInfo& physicalOperatorInfo, ExecutionContext& context) {
    auto expressionType = expression.expressionType;
    unique_ptr<ExpressionEvaluator> retVal;
    if (isExpressionLiteral(expressionType)) {
        retVal = mapLogicalLiteralExpressionToStructuredPhysical(expression, context);
    } else if (PROPERTY == expressionType || CSV_LINE_EXTRACT == expressionType) {
        /**
         * Both CSV_LINE_EXTRACT and PropertyExpression are mapped to the same physical expression
         * evaluator, because both of them only grab data from a value vector.
         */
        retVal = mapLogicalLeafExpressionToPhysical(expression, physicalOperatorInfo);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        retVal = mapLogicalExistentialSubqueryExpressionToPhysical(
            expression, physicalOperatorInfo, context);
    } else if (ALIAS == expressionType) {
        /**
         * If an alias expression has been matched before, it should be treated as a leaf
         * expression. Otherwise map the child of alias expression.
         */
        if (physicalOperatorInfo.containVariable(expression.getInternalName())) {
            retVal = mapLogicalLeafExpressionToPhysical(expression, physicalOperatorInfo);
        } else {
            retVal = mapToPhysical(*expression.children[0], physicalOperatorInfo, context);
        }
    } else if (isExpressionAggregate(expressionType)) {
        if (expressionType == COUNT_STAR_FUNC) {
            // COUNT_STAR has no child expression
            assert(expression.children.empty());
            retVal = make_unique<AggregateExpressionEvaluator>(expressionType, expression.dataType,
                AggregateExpressionEvaluator::getAggregationFunction(
                    expressionType, expression.dataType));
        } else {
            auto child = mapToPhysical(*expression.children[0], physicalOperatorInfo, context);
            retVal = make_unique<AggregateExpressionEvaluator>(expressionType, expression.dataType,
                move(child),
                AggregateExpressionEvaluator::getAggregationFunction(
                    expressionType, child->dataType));
        }
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(*expression.children[0], physicalOperatorInfo, context);
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
        auto& logicalLExpr = *expression.children[0];
        auto& logicalRExpr = *expression.children[1];
        auto castLToUnstructured =
            logicalRExpr.dataType == UNSTRUCTURED && logicalLExpr.dataType != UNSTRUCTURED;
        auto lExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            logicalLExpr, castLToUnstructured, physicalOperatorInfo, context);
        auto castRToUnstructured =
            logicalLExpr.dataType == UNSTRUCTURED && logicalRExpr.dataType != UNSTRUCTURED;
        auto rExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            logicalRExpr, castRToUnstructured, physicalOperatorInfo, context);
        retVal = make_unique<BinaryExpressionEvaluator>(
            move(lExpr), move(rExpr), expressionType, expression.dataType);
    }
    return retVal;
}

unique_ptr<ExpressionEvaluator>
ExpressionMapper::mapChildExpressionAndCastToUnstructuredIfNecessary(const Expression& expression,
    bool castToUnstructured, const PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    unique_ptr<ExpressionEvaluator> retVal;
    if (castToUnstructured && isExpressionLiteral(expression.expressionType)) {
        retVal = mapLogicalLiteralExpressionToUnstructuredPhysical(expression, context);
    } else {
        retVal = mapToPhysical(expression, physicalOperatorInfo, context);
        if (castToUnstructured) {
            retVal = make_unique<UnaryExpressionEvaluator>(
                move(retVal), CAST_TO_UNSTRUCTURED_VALUE, UNSTRUCTURED);
        }
    }
    return retVal;
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalLiteralExpressionToUnstructuredPhysical(
    const Expression& expression, ExecutionContext& context) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector =
        make_shared<ValueVector>(context.memoryManager, UNSTRUCTURED, true /* isSingleValue */);
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
    const Expression& expression, ExecutionContext& context) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        context.memoryManager, literalExpression.dataType, true /* isSingleValue */);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    switch (expression.dataType) {
    case INT64: {
        ((int64_t*)vector->values)[0] = literalExpression.literal.val.int64Val;
    } break;
    case DOUBLE: {
        ((double_t*)vector->values)[0] = literalExpression.literal.val.doubleVal;
    } break;
    case BOOL: {
        auto val = literalExpression.literal.val.booleanVal;
        vector->setNull(0, val == NULL_BOOL);
        vector->values[0] = val;
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
    const Expression& expression, const PhysicalOperatorsInfo& physicalOperatorInfo) {
    return make_unique<ExpressionEvaluator>(
        physicalOperatorInfo.getDataPos(expression.getInternalName()), expression.expressionType,
        expression.dataType);
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapLogicalExistentialSubqueryExpressionToPhysical(
    const Expression& expression, const PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& subqueryExpression = (ExistentialSubqueryExpression&)expression;
    auto prevPhysicalOperatorsInfo = planMapper->enterSubquery(&physicalOperatorInfo);
    auto subPlan = planMapper->mapToPhysical(subqueryExpression.getSubPlan(), context);
    planMapper->exitSubquery(prevPhysicalOperatorsInfo);
    return make_unique<ExistentialSubqueryEvaluator>(unique_ptr<ResultCollector>{
        dynamic_cast<ResultCollector*>(subPlan->lastOperator.release())});
}

} // namespace processor
} // namespace graphflow
