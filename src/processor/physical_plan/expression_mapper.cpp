#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/common/include/expression_type.h"
#include "src/expression/include/logical/logical_expression.h"
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
    auto expressionType = expression.getExpressionType();
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToPhysical(expression);
    } else if (isExpressionLeafVariable(expressionType)) {
        return mapLogicalPropertyExpressionToPhysical(expression, physicalOperatorInfo, dataChunks);
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalUnaryExpression>(move(child), expression.getExpressionType());
    } else if (isExpressionBinary(expressionType)) {
        auto lExpr = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo, dataChunks);
        auto rExpr = mapToPhysical(expression.getChildExpr(1), physicalOperatorInfo, dataChunks);
        return make_unique<PhysicalBinaryExpression>(
            move(lExpr), move(rExpr), expression.getExpressionType());
    }
    // should never happen.
    throw std::invalid_argument("Unsupported expression type.");
}

unique_ptr<PhysicalExpression> ExpressionMapper::clone(
    const PhysicalExpression& expression, DataChunks& dataChunks) {
    if (expression.isLiteralLeafExpression()) {
        return make_unique<PhysicalExpression>(expression.result, expression.expressionType);
    } else if (expression.isPropertyLeafExpression()) {
        auto dataChunk = dataChunks.getDataChunk(expression.dataChunkPos);
        auto valueVector = dataChunk->getValueVector(expression.valueVectorPos);
        return make_unique<PhysicalExpression>(valueVector, expression.dataChunkPos,
            expression.valueVectorPos, expression.expressionType);
    } else if (expression.getNumChildrenExpr() == 1) { // unary expression.
        return make_unique<PhysicalUnaryExpression>(
            clone(expression.getChildExpr(0), dataChunks), expression.expressionType);
    } else { // binary expression.
        return make_unique<PhysicalBinaryExpression>(clone(expression.getChildExpr(0), dataChunks),
            clone(expression.getChildExpr(1), dataChunks), expression.expressionType);
    }
}

unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysical(
    const LogicalExpression& expression) {
    auto dataType = expression.getDataType();
    // We create an owner dataChunk which is flat and of size 1 to contain the literal.
    auto dataChunkForLiteral = make_shared<DataChunk>(true /* initializeSelectedValuesPos */);
    dataChunkForLiteral->size = 1;
    dataChunkForLiteral->currPos = 0;
    auto valueVector = make_shared<ValueVector>(dataType, 1 /* capacity */);
    valueVector->setDataChunkOwner(dataChunkForLiteral);
    switch (dataType) {
    case INT32: {
        valueVector->setValue(0, expression.getLiteralValue().primitive.integer);
    } break;
    case DOUBLE: {
        valueVector->setValue(0, expression.getLiteralValue().primitive.double_);
    } break;
    case BOOL:
        valueVector->setValue(0, expression.getLiteralValue().primitive.boolean);
        break;
    default:
        throw std::invalid_argument("Unsupported data type for literal expressions.");
    }
    return make_unique<PhysicalExpression>(valueVector, expression.expressionType);
}

unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
    DataChunks& dataChunks) {
    const auto& variableName = expression.getVariableName();
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(variableName);
    auto dataChunk = dataChunks.getDataChunk(dataChunkPos);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    auto valueVector = dataChunk->getValueVector(valueVectorPos);
    return make_unique<PhysicalExpression>(
        valueVector, dataChunkPos, valueVectorPos, expression.expressionType);
}

} // namespace processor
} // namespace graphflow
