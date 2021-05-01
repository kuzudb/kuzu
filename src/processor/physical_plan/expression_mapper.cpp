#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/common/include/expression_type.h"
#include "src/expression/include/logical/logical_literal_expression.h"
#include "src/expression/include/physical/physical_binary_expression.h"
#include "src/expression/include/physical/physical_expression.h"
#include "src/expression/include/physical/physical_unary_expression.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysical(
    const LogicalExpression& expression);

static unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks);

unique_ptr<PhysicalExpression> ExpressionMapper::mapToPhysical(const LogicalExpression& expression,
    PhysicalOperatorsInfo& physicalOperatorInfo, DataChunks& dataChunks) {
    auto expressionType = expression.expressionType;
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToPhysical(expression);
    } else if (isExpressionLeafVariable(expressionType)) {
        return mapLogicalPropertyExpressionToPhysical(expression, physicalOperatorInfo, dataChunks);
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalUnaryExpression>(
            move(child), expressionType, expression.dataType);
    } else {
        assert(isExpressionBinary(expressionType));
        auto lExpr = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, dataChunks);
        auto rExpr = mapToPhysical(expression.getChildExpr(1), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalBinaryExpression>(
            move(lExpr), move(rExpr), expressionType, expression.dataType);
    }
}

unique_ptr<PhysicalExpression> ExpressionMapper::clone(
    const PhysicalExpression& expression, DataChunks& dataChunks) {
    if (isExpressionLeafLiteral(expression.expressionType)) {
        return make_unique<PhysicalExpression>(expression.result, expression.expressionType);
    } else if (isExpressionLeafVariable(expression.expressionType)) {
        auto dataChunk = dataChunks.getDataChunk(expression.dataChunkPos);
        auto valueVector = dataChunk->getValueVector(expression.valueVectorPos);
        return make_unique<PhysicalExpression>(
            valueVector, expression.dataChunkPos, expression.valueVectorPos);
    } else if (expression.getNumChildrenExpr() == 1) { // unary expression.
        return make_unique<PhysicalUnaryExpression>(clone(expression.getChildExpr(0), dataChunks),
            expression.expressionType, expression.dataType);
    } else { // binary expression.
        return make_unique<PhysicalBinaryExpression>(clone(expression.getChildExpr(0), dataChunks),
            clone(expression.getChildExpr(1), dataChunks), expression.expressionType,
            expression.dataType);
    }
}

unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysical(
    const LogicalExpression& expression) {
    auto& literalExpression = (LogicalLiteralExpression&)expression;
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto vector = make_shared<ValueVector>(
        literalExpression.storeAsPrimitiveVector ? literalExpression.dataType : UNSTRUCTURED,
        1 /* capacity */);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    if (!literalExpression.storeAsPrimitiveVector) {
        ((Value*)vector->values)[0] = literalExpression.literal;
    } else {
        switch (expression.dataType) {
        case INT32: {
            ((int32_t*)vector->values)[0] = literalExpression.literal.primitive.int32Val;
        } break;
        case DOUBLE: {
            ((double_t*)vector->values)[0] = literalExpression.literal.primitive.doubleVal;
        } break;
        case BOOL: {
            auto val = literalExpression.literal.primitive.booleanVal;
            vector->nullMask[0] = val == NULL_BOOL;
            vector->values[0] = val;
        } break;
        case STRING: {
            ((gf_string_t*)vector->values)[0] = literalExpression.literal.strVal;
        } break;
        default:
            assert(false);
        }
    }
    return make_unique<PhysicalExpression>(vector, expression.expressionType);
}

unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks) {
    const auto& variableName = expression.variableName;
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(variableName);
    auto dataChunk = dataChunks.getDataChunk(dataChunkPos);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    auto valueVector = dataChunk->getValueVector(valueVectorPos);
    return make_unique<PhysicalExpression>(valueVector, dataChunkPos, valueVectorPos);
}

} // namespace processor
} // namespace graphflow
