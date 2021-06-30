#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/binder/include/expression/literal_expression.h"
#include "src/common/include/expression_type.h"
#include "src/expression_evaluator/include/binary_expression_evaluator.h"
#include "src/expression_evaluator/include/unary_expression_evaluator.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

static unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToStructuredPhysical(
    MemoryManager& memoryManager, const Expression& expression);

static unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToUnstructuredPhysical(
    MemoryManager& memoryManager, const Expression& expression);

static unique_ptr<ExpressionEvaluator> mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
    MemoryManager& memoryManager, const Expression& expression,
    PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet);

unique_ptr<ExpressionEvaluator> ExpressionMapper::mapToPhysical(MemoryManager& memoryManager,
    const Expression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    ResultSet& resultSet) {
    auto expressionType = expression.expressionType;
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToStructuredPhysical(memoryManager, expression);
    }
    unique_ptr<ExpressionEvaluator> retVal;
    if (isExpressionLeafVariable(expressionType)) {
        /**
         *Both CSV_LINE_EXTRACT and PropertyExpression are mapped to the same physical expression
         *evaluator, because both of them only grab data from a value vector.
         */
        retVal = mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
            memoryManager, expression, physicalOperatorInfo, resultSet);
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(
            memoryManager, expression.getChildExpr(0), physicalOperatorInfo, resultSet);
        retVal = make_unique<UnaryExpressionEvaluator>(
            memoryManager, move(child), expressionType, expression.dataType);
    } else {
        assert(isExpressionBinary(expressionType));
        // If one of the children expressions has UNSTRUCTURED data type, i.e. the type of the data
        // is not fixed and can take on multiple values, then we add a cast operation to the other
        // child (even if the other child's type is known in advance and is structured). The
        // UNSTRUCTURED child's data will be stored in vectors that store boxed values instead of
        // structured (i.e., primitive) values. The cast operation ensures that the other child's
        // values are also boxed so we can call expression evaluation functions that expect two
        // boxed values.
        auto& logicalLExpr = expression.getChildExpr(0);
        auto& logicalRExpr = expression.getChildExpr(1);
        auto castLToUnstructured =
            logicalRExpr.dataType == UNSTRUCTURED && logicalLExpr.dataType != UNSTRUCTURED;
        auto lExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            memoryManager, logicalLExpr, castLToUnstructured, physicalOperatorInfo, resultSet);
        auto castRToUnstructured =
            logicalLExpr.dataType == UNSTRUCTURED && logicalRExpr.dataType != UNSTRUCTURED;
        auto rExpr = mapChildExpressionAndCastToUnstructuredIfNecessary(
            memoryManager, logicalRExpr, castRToUnstructured, physicalOperatorInfo, resultSet);
        retVal = make_unique<BinaryExpressionEvaluator>(
            memoryManager, move(lExpr), move(rExpr), expressionType, expression.dataType);
    }
    return retVal;
}

unique_ptr<ExpressionEvaluator> ExpressionMapper::clone(
    MemoryManager& memoryManager, const ExpressionEvaluator& expression, ResultSet& resultSet) {
    if (isExpressionLeafLiteral(expression.expressionType)) {
        return make_unique<ExpressionEvaluator>(
            memoryManager, expression.result, expression.expressionType);
    } else if (isExpressionLeafVariable(expression.expressionType)) {
        auto dataChunk = resultSet.dataChunks[expression.dataChunkPos];
        auto valueVector = dataChunk->getValueVector(expression.valueVectorPos);
        return make_unique<ExpressionEvaluator>(
            memoryManager, valueVector, expression.dataChunkPos, expression.valueVectorPos);
    } else if (expression.getNumChildrenExpr() == 1) { // unary expression.
        return make_unique<UnaryExpressionEvaluator>(memoryManager,
            clone(memoryManager, expression.getChildExpr(0), resultSet), expression.expressionType,
            expression.dataType);
    } else { // binary expression.
        return make_unique<BinaryExpressionEvaluator>(memoryManager,
            clone(memoryManager, expression.getChildExpr(0), resultSet),
            clone(memoryManager, expression.getChildExpr(1), resultSet), expression.expressionType,
            expression.dataType);
    }
}
unique_ptr<ExpressionEvaluator>
ExpressionMapper::mapChildExpressionAndCastToUnstructuredIfNecessary(MemoryManager& memoryManager,
    const Expression& expression, bool castToUnstructured,
    PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet) {
    unique_ptr<ExpressionEvaluator> retVal;
    if (castToUnstructured && isExpressionLeafLiteral(expression.expressionType)) {
        retVal = mapLogicalLiteralExpressionToUnstructuredPhysical(memoryManager, expression);
    } else {
        retVal = mapToPhysical(memoryManager, expression, physicalOperatorInfo, resultSet);
        if (castToUnstructured) {
            retVal = make_unique<UnaryExpressionEvaluator>(
                memoryManager, move(retVal), CAST_TO_UNSTRUCTURED_VECTOR, UNSTRUCTURED);
        }
    }
    return retVal;
}

static unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToUnstructuredPhysical(
    MemoryManager& memoryManager, const Expression& expression) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(&memoryManager, UNSTRUCTURED, true /* isSingleValue */);
    vector->state = VectorState::getSingleValueDataChunkState();
    auto& val = ((Value*)vector->values)[0];
    val.dataType = literalExpression.literal.dataType;
    switch (val.dataType) {
    case INT32: {
        val.val.int32Val = literalExpression.literal.val.int32Val;
    } break;
    case DOUBLE: {
        val.val.doubleVal = literalExpression.literal.val.doubleVal;
    } break;
    case BOOL: {
        val.val.booleanVal = literalExpression.literal.val.booleanVal;
    } break;
    case STRING: {
        vector->allocateStringOverflowSpace(
            val.val.strVal, literalExpression.literal.strVal.length());
        val.val.strVal.set(literalExpression.literal.strVal);
    } break;
    default:
        assert(false);
    }
    return make_unique<ExpressionEvaluator>(memoryManager, vector, expression.expressionType);
}

unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToStructuredPhysical(
    MemoryManager& memoryManager, const Expression& expression) {
    auto& literalExpression = (LiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        &memoryManager, literalExpression.dataType, true /* isSingleValue */);
    vector->state = VectorState::getSingleValueDataChunkState();
    switch (expression.dataType) {
    case INT32: {
        ((int32_t*)vector->values)[0] = literalExpression.literal.val.int32Val;
    } break;
    case DOUBLE: {
        ((double_t*)vector->values)[0] = literalExpression.literal.val.doubleVal;
    } break;
    case BOOL: {
        auto val = literalExpression.literal.val.booleanVal;
        vector->nullMask[0] = val == NULL_BOOL;
        vector->values[0] = val;
    } break;
    case STRING: {
        vector->addString(0 /* pos */, literalExpression.literal.strVal);
    } break;
    default:
        assert(false);
    }
    return make_unique<ExpressionEvaluator>(memoryManager, vector, expression.expressionType);
}

unique_ptr<ExpressionEvaluator> mapLogicalPropertyOrCSVLineExtractExpressionToPhysical(
    MemoryManager& memoryManager, const Expression& expression,
    PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet) {
    const auto& variableName = expression.variableName;
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(variableName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    auto dataChunk = resultSet.dataChunks[dataChunkPos];
    auto valueVector = dataChunk->getValueVector(valueVectorPos);
    return make_unique<ExpressionEvaluator>(
        memoryManager, valueVector, dataChunkPos, valueVectorPos);
}

} // namespace processor
} // namespace graphflow
