#include "src/expression/include/physical/physical_binary_expression.h"

namespace graphflow {
namespace expression {

PhysicalBinaryExpression::PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
    unique_ptr<PhysicalExpression> rightExpr, ExpressionType expressionType) {
    childrenExpr.push_back(move(leftExpr));
    childrenExpr.push_back(move(rightExpr));
    this->expressionType = expressionType;
    operation = ValueVector::getBinaryOperation(expressionType);
    result = createResultValueVector(getBinaryExpressionResultDataType(
        expressionType, childrenExpr[0]->dataType, childrenExpr[1]->dataType));
}

void PhysicalBinaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    childrenExpr[1]->evaluate();
    operation(*childrenExpr[0]->result, *childrenExpr[1]->result, *result);
}

void PhysicalBinaryExpression::setExpressionInputDataChunk(shared_ptr<DataChunk> dataChunk) {
    childrenExpr[0]->setExpressionInputDataChunk(dataChunk);
    childrenExpr[1]->setExpressionInputDataChunk(dataChunk);
}

void PhysicalBinaryExpression::setExpressionResultOwnerState(
    shared_ptr<DataChunkState> dataChunkState) {
    result->setDataChunkOwnerState(dataChunkState);
    childrenExpr[0]->setExpressionResultOwnerState(dataChunkState);
    childrenExpr[1]->setExpressionResultOwnerState(dataChunkState);
}

} // namespace expression
} // namespace graphflow
